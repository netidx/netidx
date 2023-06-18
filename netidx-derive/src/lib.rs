use std::collections::HashSet;

use proc_macro2::{token_stream, Delimiter, TokenStream, TokenTree};
use quote::{format_ident, quote, ToTokens};
use syn::{
    parse_macro_input, parse_quote, AttrStyle, Attribute, Data, DeriveInput, Field,
    Fields, GenericParam, Ident, Index,
};

fn parse_attr<R, F: FnMut(Ident, token_stream::IntoIter) -> R>(
    att: &Attribute,
    mut f: F,
) -> Option<R> {
    match att.style {
        AttrStyle::Inner(_) => None,
        AttrStyle::Outer => match att.path().segments.iter().next() {
            None => None,
            Some(seg) => match seg.ident.to_string().as_str() {
                "pack" => {
                    let tokens = att.meta.require_list().unwrap().tokens.clone();
                    let mut iter = tokens.into_iter();
                    match iter.next() {
                        Some(TokenTree::Ident(i)) => Some(f(i, iter)),
                        None | Some(_) => None,
                    }
                }
                _ => None,
            },
        },
    }
}

fn is_attr(att: &Attribute, allowed: &[&str], s: &str) -> bool {
    if !allowed.contains(&s) {
        panic!("BUG: attribute '{}' is not included in '{:?}'", s, allowed);
    }
    parse_attr(att, |i, _| {
        let i = i.to_string();
        if !allowed.contains(&i.as_str()) {
            panic!("invalid pack attribute '{}'", i)
        }
        &i == s
    })
    .unwrap_or(false)
}

fn get_tag(
    att: &Attribute,
    n: usize,
    tagged: &mut HashSet<String>,
    allowed: &[&str],
) -> Option<TokenStream> {
    parse_attr(att, |i, mut ts| {
        let i = i.to_string();
        if !allowed.contains(&i.as_str()) {
            panic!("invalid attribute '{}'", i)
        } else if i != "tag" {
            return None;
        }
        if tagged.is_empty() && n > 0 {
            panic!("all cases must be tagged or none must be")
        }
        match ts.next() {
            None => panic!("tag expected an integer argument"),
            Some(TokenTree::Group(g)) => {
                if g.delimiter() != Delimiter::Parenthesis {
                    panic!("syntax error, e.g. tag(42)")
                }
                match g.stream().into_iter().next() {
                    None => panic!("tag expected an int literal argument"),
                    Some(TokenTree::Literal(l)) => {
                        if !tagged.insert(l.to_string()) {
                            panic!("tags must be unique {} reused", l)
                        }
                        let res = TokenStream::from(TokenTree::Literal(l));
                        Some(res)
                    }
                    Some(_) => panic!("tag expected an int literal argument"),
                }
            }
            Some(_) => panic!("tag expected an integer argument"),
        }
    })
    .flatten()
}

static ENUM_ATTRS: [&'static str; 2] = ["tag", "other"];
static FIELD_ATTRS: [&'static str; 2] = ["skip", "default"];

fn encoded_len(no_wrap: bool, input: &Data) -> TokenStream {
    match input {
        Data::Struct(st) => match &st.fields {
            Fields::Named(fields) => {
                let fields = fields
                    .named
                    .iter()
                    .filter(|f| !f.attrs.iter().any(|f| is_attr(f, &FIELD_ATTRS, "skip")))
                    .map(|f| {
                        let name = &f.ident;
                        quote! {
                            netidx_core::pack::Pack::encoded_len(&self.#name)
                        }
                    });
                if no_wrap {
                    quote! {
                        0 #(+ #fields)*
                    }
                } else {
                    quote! {
                        netidx_core::pack::len_wrapped_len(0 #(+ #fields)*)
                    }
                }
            }
            Fields::Unnamed(fields) => {
                let fields = fields
                    .unnamed
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| {
                        !f.attrs.iter().any(|f| is_attr(f, &FIELD_ATTRS, "skip"))
                    })
                    .map(|(i, _)| {
                        let index = Index::from(i);
                        quote! {
                            netidx_core::pack::Pack::encoded_len(&self.#index)
                        }
                    });
                if no_wrap {
                    quote! {
                        0 #(+ #fields)*
                    }
                } else {
                    quote! {
                        netidx_core::pack::len_wrapped_len(0 #(+ #fields)*)
                    }
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
                        .filter(|f| {
                            !f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "skip"))
                        })
                        .map(|f| &f.ident);
                    let size_fields = f
                        .named
                        .iter()
                        .filter(|f| {
                            !f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "skip"))
                        })
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
                        let skip =
                            f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "skip"));
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
                        .filter(|(_, f)| {
                            !f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "skip"))
                        })
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
            if no_wrap {
                quote! {
                    1 + match self {
                        #(#cases),*
                    }
                }
            } else {
                quote! {
                    netidx_core::pack::len_wrapped_len(1 + match self {
                        #(#cases),*
                    })
                }
            }
        }
        Data::Union(_) => panic!("unions are not supported by Pack"),
    }
}

fn encode(no_wrap: bool, input: &Data) -> TokenStream {
    match input {
        Data::Struct(st) => match &st.fields {
            Fields::Named(fields) => {
                let fields = fields
                    .named
                    .iter()
                    .filter(|f| !f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "skip")))
                    .map(|f| {
                        let name = &f.ident;
                        quote! {
                            netidx_core::pack::Pack::encode(&self.#name, buf)?
                        }
                    });
                if no_wrap {
                    quote! {
                        #(#fields);*;
                        Ok(())
                    }
                } else {
                    quote! {
                        netidx_core::pack::len_wrapped_encode(buf, self, |buf| {
                            #(#fields);*;
                            Ok(())
                        })
                    }
                }
            }
            Fields::Unnamed(fields) => {
                let fields = fields
                    .unnamed
                    .iter()
                    .enumerate()
                    .filter(|(_, f)| {
                        !f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "skip"))
                    })
                    .map(|(i, _)| {
                        let index = Index::from(i);
                        quote! {
                            netidx_core::pack::Pack::encode(&self.#index, buf)?
                        }
                    });
                if no_wrap {
                    quote! {
                        #(#fields);*;
                        Ok(())
                    }
                } else {
                    quote! {
                        netidx_core::pack::len_wrapped_encode(buf, self, |buf| {
                            #(#fields);*;
                            Ok(())
                        })
                    }
                }
            }
            Fields::Unit => panic!("unit structs are not supported by Pack"),
        },
        Data::Enum(en) => {
            let mut tagged = HashSet::default();
            let cases = en.variants.iter().enumerate().map(|(i, v)| {
		let tag = &v.ident;
                let i = v
                    .attrs
                    .iter()
                    .find_map(|a| get_tag(a, i, &mut tagged, &ENUM_ATTRS))
                    .unwrap_or_else(|| if tagged.is_empty() {
			Index::from(i).to_token_stream()
		    } else {
			panic!("all cases must be tagged or none must be")
		    });
		match &v.fields {
                    Fields::Named(f) => {
			let match_fields = f
                            .named
                            .iter()
                            .filter(|f| {
				!f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "skip"))
                            })
                            .map(|f| &f.ident);
			let pack_fields = f
                            .named
                            .iter()
                            .filter(|f| {
				!f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "skip"))
                            })
                            .map(|f| {
				let name = &f.ident;
				quote! {
                                    netidx_core::pack::Pack::encode(#name, buf)?
				}
                            });
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
                            if f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "skip")) {
				format_ident!("_")
                            } else {
				format_ident!("field{}", i)
                            }
			});
			let pack_fields = f
                            .unnamed
                            .iter()
                            .enumerate()
                            .filter(|(_, f)| {
				!f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "skip"))
                            })
                            .map(|(i, _)| {
				let name = format_ident!("field{}", i);
				quote! {
                                    netidx_core::pack::Pack::encode(#name, buf)?
				}
                            });
			quote! {
                            Self::#tag(#(#match_fields),*) => {
				<u8 as netidx_core::pack::Pack>::encode(&#i, buf)?;
				#(#pack_fields);*;
				Ok(())
                            }
			}
                    }
                    Fields::Unit => {
			quote! {
                            Self::#tag => <u8 as netidx_core::pack::Pack>::encode(&#i, buf),
			}
                    }
		}
	    });
            if no_wrap {
                quote! {
                    match self {
                        #(#cases)*
                    }
                }
            } else {
                quote! {
                    netidx_core::pack::len_wrapped_encode(buf, self, |buf| {
                        match self {
                            #(#cases)*
                        }
                    })
                }
            }
        }
        Data::Union(_) => panic!("unions are not supported by Pack"),
    }
}

fn decode_default(name: &Option<Ident>) -> TokenStream {
    quote! {
        let #name = std::default::Default::default();
    }
}

fn decode_with_default(name: &Option<Ident>) -> TokenStream {
    quote! {
        let #name = match netidx_core::pack::Pack::decode(buf) {
            Ok(t) => t,
            Err(netidx_core::pack::PackError::BufferShort) =>
                std::default::Default::default(),
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
    let is_skipped = f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "skip"));
    let is_default = f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "default"));
    if is_skipped {
        decode_default(name)
    } else if is_default {
        decode_with_default(name)
    } else {
        decode_normal(name)
    }
}

fn decode_unnamed_field(f: &Field, i: usize) -> TokenStream {
    let name = Some(format_ident!("field{}", i));
    let is_skipped = f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "skip"));
    let is_default = f.attrs.iter().any(|a| is_attr(a, &FIELD_ATTRS, "default"));
    if is_skipped {
        decode_default(&name)
    } else if is_default {
        decode_with_default(&name)
    } else {
        decode_normal(&name)
    }
}

fn decode(no_wrap: bool, input: &Data) -> TokenStream {
    match input {
        Data::Struct(st) => match &st.fields {
            Fields::Named(fields) => {
                let name_fields = fields.named.iter().map(|f| &f.ident);
                let decode_fields = fields.named.iter().map(decode_named_field);
                if no_wrap {
                    quote! {
                        #(#decode_fields);*;
                        Ok(Self { #(#name_fields),* })
                    }
                } else {
                    quote! {
                        netidx_core::pack::len_wrapped_decode(buf, |buf| {
                            #(#decode_fields);*;
                            Ok(Self { #(#name_fields),* })
                        })
                    }
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
                if no_wrap {
                    quote! {
                        #(#decode_fields);*;
                        Ok(Self(#(#name_fields),*))
                    }
                } else {
                    quote! {
                        netidx_core::pack::len_wrapped_decode(buf, |buf| {
                            #(#decode_fields);*;
                            Ok(Self(#(#name_fields),*))
                        })
                    }
                }
            }
            Fields::Unit => panic!("unit structs are not supported by Pack"),
        },
        Data::Enum(en) => {
            let mut other: Option<TokenStream> = None;
            let mut tagged = HashSet::default();
            let cases = en
                .variants
                .iter()
                .enumerate()
                .map(|(i, v)| {
                    let tag = &v.ident;
                    let i = v
                        .attrs
                        .iter()
                        .find_map(|a| get_tag(a, i, &mut tagged, &ENUM_ATTRS))
                        .unwrap_or_else(|| {
                            if tagged.is_empty() {
                                Index::from(i).to_token_stream()
                            } else {
                                panic!("all cases must be tagged or none must be")
                            }
                        });
                    match &v.fields {
                        Fields::Named(f) => {
                            if v.attrs.iter().any(|a| is_attr(a, &ENUM_ATTRS, "other")) {
                                panic!(
                                    "other attribute must be applied to a unit variant"
                                )
                            }
                            let name_fields = f.named.iter().map(|f| &f.ident);
                            let decode_fields = f.named.iter().map(decode_named_field);
                            #[rustfmt::skip]
                            quote! {
				#i => {
                                    #(#decode_fields);*;
                                    Ok(Self::#tag { #(#name_fields),* })
				}
                            }
                        }
                        Fields::Unnamed(f) => {
                            if v.attrs.iter().any(|a| is_attr(a, &ENUM_ATTRS, "other")) {
                                panic!(
                                    "other attribute must be applied to a unit variant"
                                )
                            }
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
                            #[rustfmt::skip]
                            quote! {
				#i => {
                                    #(#decode_fields);*;
                                    Ok(Self::#tag(#(#name_fields),*))
				}
                            }
                        }
                        Fields::Unit => {
                            if v.attrs.iter().any(|a| is_attr(a, &ENUM_ATTRS, "other")) {
                                if other.is_some() {
                                    panic!(
                                        "other attribute may be specified at most once"
                                    )
                                }
                                other = Some(quote! { _ => Ok(Self::#tag) })
                            }
                            quote! { #i => Ok(Self::#tag), }
                        }
                    }
                })
                .collect::<Vec<_>>();
            if no_wrap {
                match other {
                    Some(other) => quote! {
                        match <u8 as netidx_core::pack::Pack>::decode(buf)? {
                            #(#cases)*
                            #other
                        }
                    },
                    None => quote! {
                        match <u8 as netidx_core::pack::Pack>::decode(buf)? {
                            #(#cases)*
                            _ => Err(netidx_core::pack::PackError::UnknownTag)
                        }
                    },
                }
            } else {
                match other {
                    Some(other) => quote! {
                        netidx_core::pack::len_wrapped_decode(buf, |buf| {
                            match <u8 as netidx_core::pack::Pack>::decode(buf)? {
                                #(#cases)*
                                #other
                            }
                        })
                    },
                    None => quote! {
                        netidx_core::pack::len_wrapped_decode(buf, |buf| {
                            match <u8 as netidx_core::pack::Pack>::decode(buf)? {
                                #(#cases)*
                                _ => Err(netidx_core::pack::PackError::UnknownTag)
                            }
                        })
                    },
                }
            }
        }
        Data::Union(_) => panic!("unions are not supported by Pack"),
    }
}

#[proc_macro_derive(Pack, attributes(pack))]
pub fn derive_pack(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let no_wrap = input.attrs.iter().any(|a| is_attr(a, &["unwrapped"], "unwrapped"));
    let name = input.ident;
    for param in &mut input.generics.params {
        if let GenericParam::Type(typ) = param {
            typ.bounds.push(parse_quote!(netidx_core::pack::Pack))
        }
    }
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let encoded_len = encoded_len(no_wrap, &input.data);
    let encode = encode(no_wrap, &input.data);
    let decode = decode(no_wrap, &input.data);
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
