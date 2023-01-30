use proc_macro2::{TokenStream, TokenTree};
use quote::{format_ident, quote};
use syn::{
    parse_macro_input, parse_quote, AttrStyle, Attribute, Data, DeriveInput, Field,
    Fields, GenericParam, Ident, Index, Type,
};

fn is_attr(att: &Attribute, s: &str) -> bool {
    match att.style {
        AttrStyle::Inner(_) => false,
        AttrStyle::Outer => {
            if let Some(seg) = att.path.segments.iter().next() {
                seg.ident.to_string() == "pack"
                    && match att.tokens.clone().into_iter().next() {
                        None => false,
                        Some(TokenTree::Group(g)) => {
                            match g.stream().into_iter().next() {
                                None => false,
                                Some(TokenTree::Ident(i)) => i.to_string() == s,
                                Some(_) => false,
                            }
                        }
                        Some(_) => false,
                    }
            } else {
                false
            }
        }
    }
}

fn encoded_len(input: &Data) -> TokenStream {
    match input {
        Data::Struct(st) => match &st.fields {
            Fields::Named(fields) => {
                let fields = fields
                    .named
                    .iter()
                    .filter(|f| !f.attrs.iter().any(|f| is_attr(f, "skip")))
                    .map(|f| {
                        let name = &f.ident;
                        quote! {
                            netidx_core::pack::Pack::encoded_len(&self.#name)
                        }
                    });
                quote! {
                    netidx_core::pack::len_wrapped_len(0 #(+ #fields)*)
                }
            }
            Fields::Unnamed(fields) => {
                let fields = fields
                    .unnamed
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| !f.attrs.iter().any(|f| is_attr(f, "skip")))
                    .map(|(i, _)| {
                        let index = Index::from(i);
                        quote! {
                            netidx_core::pack::Pack::encoded_len(&self.#index)
                        }
                    });
                quote! {
                    netidx_core::pack::len_wrapped_len(0 #(+ #fields)*)
                }
            }
            Fields::Unit => panic!("unit structs are not supported by Pack"),
        },
        Data::Enum(en) => {
            let cases = en.variants.iter().map(|v| match &v.fields {
                Fields::Named(f) => {
                    let match_fields = f
                        .named
                        .iter()
                        .filter(|f| !f.attrs.iter().any(|a| is_attr(a, "skip")))
                        .map(|f| &f.ident);
                    let size_fields = f
                        .named
                        .iter()
                        .filter(|f| !f.attrs.iter().any(|a| is_attr(a, "skip")))
                        .map(|f| {
                            let name = &f.ident;
                            quote! {
                                netidx_core::pack::Pack::encoded_len(#name)
                            }
                        });
                    let tag = &v.ident;
                    quote! {
                        Self::#tag { #(#match_fields),*, .. } => { 0 #(+ #size_fields)* }
                    }
                }
                Fields::Unnamed(f) => {
                    let match_fields = f.unnamed.iter().enumerate().map(|(i, f)| {
                        let skip = f.attrs.iter().any(|a| is_attr(a, "skip"));
                        if skip {
                            format_ident!("_")
                        } else {
                            format_ident!("field{}", i)
                        }
                    });
                    let size_fields = f
                        .unnamed
                        .iter()
                        .enumerate()
                        .filter(|(_, f)| !f.attrs.iter().any(|a| is_attr(a, "skip")))
                        .map(|(i, _)| {
                            let name = format_ident!("field{}", i);
                            quote! {
                                netidx_core::pack::Pack::encoded_len(#name)
                            }
                        });
                    let tag = &v.ident;
                    quote! {
                        Self::#tag(#(#match_fields),*) => { 0 #(+ #size_fields)* }
                    }
                }
                Fields::Unit => {
                    let tag = &v.ident;
                    quote! { Self::#tag => 0 }
                }
            });
            quote! {
                netidx_core::pack::len_wrapped_len(1 + match self {
                    #(#cases),*
                })
            }
        }
        Data::Union(_) => panic!("unions are not supported by Pack"),
    }
}

fn encode(input: &Data) -> TokenStream {
    match input {
        Data::Struct(st) => match &st.fields {
            Fields::Named(fields) => {
                let fields = fields
                    .named
                    .iter()
                    .filter(|f| !f.attrs.iter().any(|a| is_attr(a, "skip")))
                    .map(|f| {
                        let name = &f.ident;
                        quote! {
                            netidx_core::pack::Pack::encode(&self.#name, buf)?
                        }
                    });
                quote! {
                    netidx_core::pack::len_wrapped_encode(buf, self, |buf| {
                        #(#fields);*;
                        Ok(())
                    })
                }
            }
            Fields::Unnamed(fields) => {
                let fields = fields
                    .unnamed
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| !f.attrs.iter().any(|a| is_attr(a, "skip")))
                    .map(|(i, _)| {
                        let index = Index::from(i);
                        quote! {
                            netidx_core::pack::Pack::encode(&self.#index, buf)?
                        }
                    });
                quote! {
                    netidx_core::pack::len_wrapped_encode(buf, self, |buf| {
                        #(#fields);*;
                        Ok(())
                    })
                }
            }
            Fields::Unit => panic!("unit structs are not supported by Pack"),
        },
        Data::Enum(en) => {
            let cases = en.variants.iter().enumerate().map(|(i, v)| match &v.fields {
                Fields::Named(f) => {
                    let match_fields = f
                        .named
                        .iter()
                        .filter(|f| !f.attrs.iter().any(|a| is_attr(a, "skip")))
                        .map(|f| &f.ident);
                    let pack_fields = f
                        .named
                        .iter()
                        .filter(|f| !f.attrs.iter().any(|a| is_attr(a, "skip")))
                        .map(|f| {
                            let name = &f.ident;
                            quote! {
                                netidx_core::pack::Pack::encode(#name, buf)?
                            }
                        });
                    let tag = &v.ident;
                    let i = Index::from(i);
                    quote! {
                        Self::#tag { #(#match_fields),*, .. } => {
                            <u8 as netidx_core::pack::Pack>::encode(&#i, buf)?;
                            #(#pack_fields);*;
                            Ok(())
                        }
                    }
                }
                Fields::Unnamed(f) => {
                    let match_fields = f.unnamed.iter().enumerate().map(|(i, f)| {
                        if f.attrs.iter().any(|a| is_attr(a, "skip")) {
                            format_ident!("_")
                        } else {
                            format_ident!("field{}", i)
                        }
                    });
                    let pack_fields = f
                        .unnamed
                        .iter()
                        .enumerate()
                        .filter(|(_, f)| !f.attrs.iter().any(|a| is_attr(a, "skip")))
                        .map(|(i, _)| {
                            let name = format_ident!("field{}", i);
                            quote! {
                                netidx_core::pack::Pack::encode(#name, buf)?
                            }
                        });
                    let tag = &v.ident;
                    let i = Index::from(i);
                    quote! {
                        Self::#tag(#(#match_fields),*) => {
                            <u8 as netidx_core::pack::Pack>::encode(&#i, buf)?;
                            #(#pack_fields);*;
                            Ok(())
                        }
                    }
                }
                Fields::Unit => {
                    let tag = &v.ident;
                    let i = Index::from(i);
                    quote! {
                        Self::#tag => <u8 as netidx_core::pack::Pack>::encode(&#i, buf),
                    }
                }
            });
            quote! {
                netidx_core::pack::len_wrapped_encode(buf, self, |buf| {
                    match self {
                        #(#cases)*
                    }
                })
            }
        }
        Data::Union(_) => panic!("unions are not supported by Pack"),
    }
}

fn decode_default(typ: &Type, name: &Option<Ident>) -> TokenStream {
    quote! {
        let #name = #typ::default();
    }
}

fn decode_with_default(typ: &Type, name: &Option<Ident>) -> TokenStream {
    quote! {
        let #name = match netidx_core::pack::Pack::decode(buf) {
            Ok(t) => t,
            Err(netidx_core::pack::PackError::BufferShort) =>
                #typ::default(),
            Err(e) => return Err(e),
        };
    }
}

fn decode_normal(name: &Option<Ident>) -> TokenStream {
    quote! {
        let #name = netidx_core::pack::Pack::decode(buf)?
    }
}

fn decode_named_field(f: &Field) -> TokenStream {
    let name = &f.ident;
    let typ = &f.ty;
    let is_skipped = f.attrs.iter().any(|a| is_attr(a, "skip"));
    let is_default = f.attrs.iter().any(|a| is_attr(a, "default"));
    if is_skipped {
        decode_default(typ, name)
    } else if is_default {
        decode_with_default(typ, name)
    } else {
        decode_normal(name)
    }
}

fn decode_unnamed_field(f: &Field, i: usize) -> TokenStream {
    let name = Some(format_ident!("field{}", i));
    let typ = &f.ty;
    let is_skipped = f.attrs.iter().any(|a| is_attr(a, "skip"));
    let is_default = f.attrs.iter().any(|a| is_attr(a, "default"));
    if is_skipped {
        decode_default(typ, &name)
    } else if is_default {
        decode_with_default(typ, &name)
    } else {
        decode_normal(&name)
    }
}

fn decode(input: &Data) -> TokenStream {
    match input {
        Data::Struct(st) => match &st.fields {
            Fields::Named(fields) => {
                let name_fields = fields.named.iter().map(|f| &f.ident);
                let decode_fields = fields.named.iter().map(decode_named_field);
                quote! {
                    netidx_core::pack::len_wrapped_decode(buf, |buf| {
                        #(#decode_fields);*;
                        Ok(Self { #(#name_fields),* })
                    })
                }
            }
            Fields::Unnamed(fields) => {
                let name_fields = fields
                    .unnamed
                    .iter()
                    .enumerate()
                    .map(|(i, _)| format_ident!("field{}", i));
                let decode_fields = fields
                    .unnamed
                    .iter()
                    .enumerate()
                    .map(|(i, f)| decode_unnamed_field(f, i));
                quote! {
                    netidx_core::pack::len_wrapped_decode(buf, |buf| {
                        #(#decode_fields);*;
                        Ok(Self(#(#name_fields),*))
                    })
                }
            }
            Fields::Unit => panic!("unit structs are not supported by Pack"),
        },
        Data::Enum(en) => {
            let cases = en.variants.iter().enumerate().map(|(i, v)| match &v.fields {
                Fields::Named(f) => {
                    let name_fields = f.named.iter().map(|f| &f.ident);
                    let decode_fields = f.named.iter().map(decode_named_field);
                    let tag = &v.ident;
                    let i = Index::from(i);
                    quote! {
                        #i => {
                            #(#decode_fields);*;
                            Ok(Self::#tag { #(#name_fields),* })
                        }
                    }
                }
                Fields::Unnamed(f) => {
                    let name_fields = f
                        .unnamed
                        .iter()
                        .enumerate()
                        .map(|(i, _)| format_ident!("field{}", i));
                    let decode_fields = f
                        .unnamed
                        .iter()
                        .enumerate()
                        .map(|(i, f)| decode_unnamed_field(f, i));
                    let tag = &v.ident;
                    let i = Index::from(i);
                    quote! {
                        #i => {
                            #(#decode_fields);*;
                            Ok(Self::#tag(#(#name_fields),*))
                        }
                    }
                }
                Fields::Unit => {
                    let tag = &v.ident;
                    let i = Index::from(i);
                    quote! { #i => Ok(Self::#tag), }
                }
            });
            quote! {
                netidx_core::pack::len_wrapped_decode(buf, |buf| {
                    match <u8 as netidx_core::pack::Pack>::decode(buf)? {
                        #(#cases)*
                        _ => Err(netidx_core::pack::PackError::UnknownTag)
                    }
                })
            }
        }
        Data::Union(_) => panic!("unions are not supported by Pack"),
    }
}

#[proc_macro_derive(Pack, attributes(pack))]
pub fn derive_pack(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    for param in &mut input.generics.params {
        if let GenericParam::Type(typ) = param {
            typ.bounds.push(parse_quote!(netidx_core::pack::Pack))
        }
    }
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let encoded_len = encoded_len(&input.data);
    let encode = encode(&input.data);
    let decode = decode(&input.data);
    let expanded = quote! {
        impl #impl_generics netidx_core::pack::Pack for #name #ty_generics #where_clause {
            fn encoded_len(&self) -> usize {
                #encoded_len
            }

            fn encode(
                &self,
                buf: &mut impl bytes::BufMut
            ) -> std::result::Result<(), netidx_core::pack::PackError> {
                #encode
            }

            fn decode(
                buf: &mut impl bytes::Buf
            ) -> std::result::Result<Self, netidx_core::pack::PackError> {
                #decode
            }
        }
    };
    proc_macro::TokenStream::from(expanded)
}
