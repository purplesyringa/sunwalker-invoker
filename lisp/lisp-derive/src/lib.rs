#[macro_use]
extern crate quote;

use darling::FromDeriveInput;
use proc_macro::TokenStream;
use syn::parse_macro_input;
use syn::{Data, DeriveInput, Fields};

#[derive(FromDeriveInput)]
#[darling(attributes(lisp))]
struct Options {
    name: String,
}

#[proc_macro_derive(LispType, attributes(lisp))]
pub fn derive_trait(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let options = Options::from_derive_input(&input).expect("Wrong options");
    let name = input.ident;

    let n_fields;
    let fields = match input.data {
        Data::Struct(data) => match data.fields {
            Fields::Named(fields) => {
                n_fields = fields.named.len();
                let recurse = fields.named.iter().map(|field| {
                    let name = &field.ident;
                    quote! {
                        #name: ::lisp::evaluate(it.next().unwrap(), state)?.to_native()?
                    }
                });
                quote! {
                    #(#recurse,)*
                }
            }
            Fields::Unnamed(_) => unimplemented!(),
            Fields::Unit => unimplemented!(),
        },
        Data::Enum(_) | Data::Union(_) => unimplemented!(),
    };

    let lisp_name = options.name;
    let cast_fn_name = syn::Ident::new(format!("_cast_{}", lisp_name).as_ref(), name.span());
    let add_fn_name = syn::Ident::new(format!("_add_{}", lisp_name).as_ref(), name.span());

    let lisp_name_s = lisp_name.to_string();
    let lisp_name = quote! { #lisp_name };

    let expanded = quote! {
        fn #cast_fn_name(call: ::lisp::term::CallTerm, state: &::lisp::State) -> ::std::result::Result<::lisp::TypedRef, ::lisp::Error> {
            if call.params.len() == #n_fields {
                let mut it = call.params.into_iter();
                Ok(::lisp::TypedRef::new(#name { #fields }))
            } else {
                Err(::lisp::Error{message: format!("Expected {} arguments to ({} ...)", #n_fields, #lisp_name)})
            }
        }

        #[::lisp::ctor]
        fn #add_fn_name() {
            ::lisp::evaluate::USER_FNS_HASHMAP.write().unwrap().insert(#lisp_name_s, #cast_fn_name);
        }

        impl ::lisp::LispType for #name {
            fn get_type_name() -> String {
                #lisp_name_s.to_string()
            }
        }

        impl ::lisp::NativeType for #name {
            fn from_lisp_ref(value: &::lisp::TypedRef) -> ::std::result::Result<Self, ::lisp::Error> {
                value.to_concrete::<Self>()
            }
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro_attribute]
pub fn function(_meta: TokenStream, input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::ItemFn);
    let name = &input.sig.ident;

    // let call_fn_name = syn::Ident::new(format!("_call_{}", name).as_ref(), name.span());
    let add_fn_name = syn::Ident::new(format!("_add_{}", name).as_ref(), name.span());

    let lisp_name_s = name.to_string();

    let expanded = quote! {
        #input

        #[::lisp::ctor]
        fn #add_fn_name() {
            ::lisp::evaluate::USER_FNS_HASHMAP.write().unwrap().insert(#lisp_name_s, #name);
        }
    };

    TokenStream::from(expanded)
}
