#[macro_use]
extern crate quote;

use proc_macro::TokenStream;
use quote::ToTokens;
use syn::parse_macro_input;
use syn::DeriveInput;

#[proc_macro_attribute]
pub fn entrypoint(_meta: TokenStream, input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as syn::ItemFn);

    let tokio_attr_index = input.attrs.iter().position(|attr| {
        let path = &attr.path;
        (quote! {#path}).to_string().contains("tokio :: main")
    });
    let tokio_attr = tokio_attr_index.map(|i| input.attrs.remove(i));

    let return_type = match input.sig.output {
        syn::ReturnType::Default => quote! { () },
        syn::ReturnType::Type(_, ref ty) => quote! { #ty },
    };

    // Pray all &input are distinct
    let link_name = format!(
        "multiprocessing_{}_{:?}",
        input.sig.ident.to_string(),
        &input as *const syn::ItemFn
    );
    let fn_name_ident = format_ident!("f_{}", link_name);
    let real_entry_ident = format_ident!("e_{}", link_name);
    let args_struct_name_ident = format_ident!("S_{}", link_name);

    let ident = input.sig.ident;
    input.sig.ident = format_ident!("call");

    input.vis = syn::Visibility::Public(syn::VisPublic {
        pub_token: <syn::Token![pub] as std::default::Default>::default(),
    });

    let args = &input.sig.inputs;

    let mut fn_args = Vec::new();
    let mut extracted_args = Vec::new();
    let mut arg_names = Vec::new();
    for arg in args {
        if let syn::FnArg::Typed(pattype) = arg {
            if let syn::Pat::Ident(ref patident) = *pattype.pat {
                let ident = &patident.ident;
                let colon_token = &pattype.colon_token;
                let ty = &pattype.ty;
                fn_args.push(quote! { #ident #colon_token #ty });
                extracted_args.push(quote! { multiprocessing_args.#ident });
                arg_names.push(quote! { #ident });
            } else {
                unreachable!();
            }
        } else {
            unreachable!();
        }
    }

    let entrypoint;

    if let Some(tokio_attr) = tokio_attr {
        entrypoint = quote! {
            #tokio_attr
            async fn #real_entry_ident(input_rx_fd: ::std::os::unix::io::RawFd, output_tx_fd: ::std::os::unix::io::RawFd) -> i32 {
                use ::std::os::unix::io::FromRawFd;
                let mut input_rx = unsafe {
                    ::multiprocessing::tokio::Receiver::<#args_struct_name_ident>::from_raw_fd(input_rx_fd)
                };
                let mut output_tx = unsafe {
                    ::multiprocessing::tokio::Sender::<#return_type>::from_raw_fd(output_tx_fd)
                };
                let multiprocessing_args = input_rx.recv().await.unwrap().unwrap();
                output_tx.send(&#ident::call( #(#extracted_args,)* ).await).await.unwrap();
                0
            }
        };
    } else {
        entrypoint = quote! {
            fn #real_entry_ident(input_rx_fd: ::std::os::unix::io::RawFd, output_tx_fd: ::std::os::unix::io::RawFd) -> i32 {
                use ::std::os::unix::io::FromRawFd;
                let mut input_rx = unsafe {
                    ::multiprocessing::Receiver::<#args_struct_name_ident>::from_raw_fd(input_rx_fd)
                };
                let mut output_tx = unsafe {
                    ::multiprocessing::Sender::<#return_type>::from_raw_fd(output_tx_fd)
                };
                let multiprocessing_args = input_rx.recv().unwrap().unwrap();
                output_tx.send(&#ident::call( #(#extracted_args,)* )).unwrap();
                0
            }
        };
    }

    let expanded = quote! {
        #[derive(::multiprocessing::SerializeSafe)]
        struct #args_struct_name_ident {
            #(#fn_args,)*
        }

        #entrypoint

        #[::multiprocessing::imp::ctor]
        fn #fn_name_ident() {
            ::multiprocessing::imp::ENTRY_POINTS.write().unwrap().insert(#link_name, #real_entry_ident);
        }

        #[allow(non_camel_case_types)]
        struct #ident {
        }

        impl #ident {
            #[link_name = #link_name]
            #input

            pub fn spawn(#(#fn_args,)*) -> ::std::io::Result<::multiprocessing::Child<#return_type>> {
                use ::std::os::unix::{process::CommandExt, io::AsRawFd};

                let (mut input_tx, mut input_rx) = ::multiprocessing::channel::<#args_struct_name_ident>()?;
                let input_rx_fd = input_rx.as_raw_fd();

                let (mut output_tx, mut output_rx) = ::multiprocessing::channel::<#return_type>()?;
                let output_tx_fd = output_tx.as_raw_fd();

                let mut command = ::std::process::Command::new("/proc/self/exe");
                let child = unsafe {
                    command
                        .arg0(#link_name)
                        .arg(input_rx_fd.to_string())
                        .arg(output_tx_fd.to_string())
                        .pre_exec(move || {
                            ::multiprocessing::imp::disable_cloexec(input_rx_fd)?;
                            ::multiprocessing::imp::disable_cloexec(output_tx_fd)?;
                            Ok(())
                        })
                        .spawn()?
                };

                input_tx.send(
                    &#args_struct_name_ident {
                        #(#arg_names,)*
                    }
                )?;

                Ok(::multiprocessing::Child::new(child, output_rx))
            }
        }

        impl ::multiprocessing::Entrypoint for #ident {
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn main(_meta: TokenStream, input: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(input as syn::ItemFn);

    input.sig.ident = syn::Ident::new("multiprocessing_old_main", input.sig.ident.span());

    let expanded = quote! {
        #input

        #[::multiprocessing::imp::ctor]
        fn multiprocessing_add_main() {
            *::multiprocessing::imp::MAIN_ENTRY.write().unwrap() = Some(|| {
                ::multiprocessing::imp::Report::report(multiprocessing_old_main())
            });
        }

        fn main() {
            ::multiprocessing::imp::main()
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(SerializeSafe)]
pub fn derive_serialize_safe(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);

    let ident = &input.ident;

    let generics = {
        let params: Vec<_> = input
            .generics
            .params
            .iter()
            .map(|param| match param {
                syn::GenericParam::Type(ref ty) => ty.ident.to_token_stream(),
                syn::GenericParam::Lifetime(ref lt) => lt.lifetime.to_token_stream(),
                syn::GenericParam::Const(ref con) => con.ident.to_token_stream(),
            })
            .collect();
        quote! { <#(#params,)*> }
    };

    let generics_impl = {
        let params = input.generics.params;
        quote! { <#params> }
    };

    let generics_where = input.generics.where_clause;

    let expanded = match input.data {
        syn::Data::Struct(struct_) => match struct_.fields {
            syn::Fields::Named(fields) => {
                let serialize_fields = fields.named.iter().map(|field| {
                    let ident = &field.ident;
                    quote! {
                        s.serialize(&self.#ident);
                    }
                });
                let deserialize_fields = fields.named.iter().map(|field| {
                    let ident = &field.ident;
                    quote! {
                        #ident: d.deserialize(),
                    }
                });
                quote! {
                    impl #generics_impl ::multiprocessing::SerializeSafe for #ident #generics #generics_where {
                        fn serialize_self(&self, s: &mut ::multiprocessing::Serializer) {
                            #(#serialize_fields)*
                        }
                        fn deserialize_self(d: &mut ::multiprocessing::Deserializer) -> Self {
                            Self {
                                #(#deserialize_fields)*
                            }
                        }
                    }
                }
            }
            syn::Fields::Unnamed(fields) => {
                let serialize_fields = fields.unnamed.iter().enumerate().map(|(i, _)| {
                    quote! {
                        s.serialize(&self.#i);
                    }
                });
                let deserialize_fields = fields.unnamed.iter().map(|_| {
                    quote! {
                        d.deserialize(),
                    }
                });
                quote! {
                    impl #generics_impl ::multiprocessing::SerializeSafe for #ident #generics #generics_where {
                        fn serialize_self(&self, s: &mut ::multiprocessing::Serializer) {
                            #(#serialize_fields)*
                        }
                        fn deserialize_self(d: &mut ::multiprocessing::Deserializer) -> Self {
                            Self(
                                #(#deserialize_fields)*
                            )
                        }
                    }
                }
            }
            syn::Fields::Unit => {
                quote! {
                    impl #generics_impl ::multiprocessing::SerializeSafe for #ident #generics #generics_where {
                        fn serialize_self(&self, s: &mut ::multiprocessing::Serializer) {
                            s.serialize(&self.0);
                        }
                        fn deserialize_self(d: &mut ::multiprocessing::Deserializer) -> Self {
                            Self(d.deserialize())
                        }
                    }
                }
            }
        },
        syn::Data::Enum(enum_) => {
            let serialize_variants = enum_.variants.iter().enumerate().map(|(i, variant)| {
                let ident = &variant.ident;
                let (mut refs, sers): (Vec<_>, Vec<_>) = variant
                    .fields
                    .iter()
                    .enumerate()
                    .map(|(i, _)| {
                        let ident = format_ident!("a{}", i);
                        (quote! { ref #ident }, quote! { s.serialize(#ident); })
                    })
                    .unzip();
                let fields = if variant.fields.is_empty() {
                    quote! {}
                } else {
                    let first_ref = refs.remove(0);
                    quote! { (#first_ref #(,#refs)*) }
                };
                quote! {
                    Self::#ident #fields => {
                        s.serialize(&(#i as usize));
                        #(#sers)*
                    }
                }
            });
            let deserialize_variants = enum_.variants.iter().enumerate().map(|(i, variant)| {
                let ident = &variant.ident;
                if variant.fields.is_empty() {
                    quote! { #i => Self::#ident }
                } else {
                    let des: Vec<_> = variant
                        .fields
                        .iter()
                        .map(|attr| {
                            let ident = &attr.ident;
                            quote! { d.deserialize(#ident) }
                        })
                        .collect();
                    quote! { #i => Self::#ident(#(#des,)*) }
                }
            });
            quote! {
                impl #generics_impl ::multiprocessing::SerializeSafe for #ident #generics #generics_where {
                    fn serialize_self(&self, s: &mut ::multiprocessing::Serializer) {
                        match self {
                            #(#serialize_variants,)*
                        }
                    }
                    fn deserialize_self(d: &mut ::multiprocessing::Deserializer) -> Self {
                        match d.deserialize::<usize>() {
                            #(#deserialize_variants,)*
                            _ => panic!("Unexpected enum variant"),
                        }
                    }
                }
            }
        }
        syn::Data::Union(_) => unimplemented!(),
    };

    TokenStream::from(expanded)
}
