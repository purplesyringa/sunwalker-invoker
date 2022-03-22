#[macro_use]
extern crate quote;

use darling::FromDeriveInput;
use proc_macro::TokenStream;
use std::collections::HashMap;
use syn::spanned::Spanned;
use syn::{parse_macro_input, parse_quote};
use syn::{Data, DeriveInput, Fields};

#[derive(FromDeriveInput)]
#[darling(attributes(lisp))]
struct Options {
    name: String,
}

#[proc_macro_derive(TryFromTerm, attributes(lisp))]
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
                        #name: ::lisp::evaluate(it.next().unwrap())?
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
        fn #cast_fn_name(call: ::lisp::term::CallTerm) -> ::std::result::Result<Box<dyn std::any::Any>, ::lisp::Error> {
            if call.params.len() == #n_fields {
                let mut it = call.params.into_iter();
                Ok(Box::new(#name { #fields }))
            } else {
                Err(::lisp::Error{message: format!("Expected {} arguments to ({} ...)", #n_fields, #lisp_name)})
            }
        }

        #[::lisp::ctor]
        fn #add_fn_name() {
            ::lisp::evaluate::USER_FNS_HASHMAP.write().unwrap().insert(#lisp_name_s, #cast_fn_name);
        }
    };

    TokenStream::from(expanded)
}

#[proc_macro]
pub fn generic_functions(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as syn::Block);

    let mut trait_definitions = Vec::new();

    let mut definitions_by_name = HashMap::new();
    for stmt in input.stmts.into_iter() {
        if let syn::Stmt::Item(item) = stmt {
            if let syn::Item::Fn(func) = item {
                let name = &func.sig.ident;
                let name_s = name.to_string();

                definitions_by_name.insert(name_s.clone(), Vec::new());
                let definitions = definitions_by_name.get_mut(name_s.as_str()).unwrap();
                definitions.push(func);
            } else {
                return TokenStream::from(
                    quote_spanned! { item.span() => compile_error!("expected fn"); },
                );
            }
        } else {
            return TokenStream::from(
                quote_spanned! { stmt.span() => compile_error!("expected fn"); },
            );
        }
    }

    let mut cases = Vec::new();

    for (name, definitions) in definitions_by_name.into_iter() {
        let trait_name =
            syn::Ident::new(format!("TryCallIfDefined_{}", name).as_ref(), name.span());

        trait_definitions.push(quote! {
            trait #trait_name {
                fn try_call(call: crate::term::CallTerm) -> Result<Self, Error> where Self: Sized;
            }
            impl<T: Sized> #trait_name for T {
                default fn try_call(call: crate::term::CallTerm) -> Result<Self, Error> {
                    Err(Error{message: format!("No overload of {} returns {}", #name, ::std::any::type_name::<T>())})
                }
            }
        });

        for (i, definition) in definitions.into_iter().enumerate() {
            let mut sig = definition.sig;
            let block = definition.block;

            let generics = &sig.generics;
            let new_ident =
                syn::Ident::new(format!("{}_{}", sig.ident, i).as_ref(), sig.ident.span());
            sig.ident = new_ident.clone();
            let output = match &mut sig.output {
                syn::ReturnType::Default => {
                    return TokenStream::from(
                        quote_spanned! { sig.span() => compile_error!("need to specify return type"); },
                    );
                }
                syn::ReturnType::Type(_, ty) => {
                    let old_ty = ty.clone();
                    *ty = parse_quote! { Result<#ty, Error> };
                    old_ty
                }
            };

            trait_definitions.push(quote! {
                #sig #block

                impl #generics #trait_name for #output {
                    fn try_call(call: crate::term::CallTerm) -> Result<Self, Error> {
                        #new_ident(call)
                    }
                }
            });
        }

        cases.push(quote! {
            #name => <T as #trait_name>::try_call(call)
        });
    }

    let expanded = quote! {
        #(#trait_definitions)*

        pub(crate) fn evaluate_call_builtin<T>(call: crate::term::CallTerm) -> Result<T, Error> {
            match call.name.as_ref() {
                #(#cases,)*
                name => Err(Error { message: format!("Function {} is not defined", name) }),
            }
        }
    };

    TokenStream::from(expanded)
}
