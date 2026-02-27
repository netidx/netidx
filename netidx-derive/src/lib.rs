use std::collections::HashSet;

use proc_macro2::{token_stream, Delimiter, TokenStream, TokenTree};
use quote::{format_ident, quote, ToTokens};
use syn::{
    parse_macro_input, parse_quote, AttrStyle, Attribute, Data, DeriveInput, Field,
    Fields, GenericParam, Ident, Index,
};

fn parse_attr<R, F: FnMut(Ident, token_stream::IntoIter) -> R>(
    att: &Attribute,
    attr_ns: &str,
    mut f: F,
) -> Option<R> {
    match att.style {
        AttrStyle::Inner(_) => None,
        AttrStyle::Outer => match att.path().segments.iter().next() {
            None => None,
            Some(seg) if seg.ident == attr_ns => {
                let tokens = att.meta.require_list().unwrap().tokens.clone();
                let mut iter = tokens.into_iter();
                match iter.next() {
                    Some(TokenTree::Ident(i)) => Some(f(i, iter)),
                    None | Some(_) => None,
                }
            }
            _ => None,
        },
    }
}

fn is_attr(att: &Attribute, attr_ns: &str, allowed: &[&str], s: &str) -> bool {
    if !allowed.contains(&s) {
        panic!("BUG: attribute '{}' is not included in '{:?}'", s, allowed);
    }
    parse_attr(att, attr_ns, |i, _| {
        let i = i.to_string();
        if !allowed.contains(&i.as_str()) {
            panic!("invalid {} attribute '{}'", attr_ns, i)
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
    parse_attr(att, "pack", |i, mut ts| {
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
static VALUE_FIELD_ATTRS: [&'static str; 3] = ["rename", "skip", "default"];
static VALUE_VARIANT_ATTRS: [&'static str; 1] = ["rename"];

fn encoded_len(no_wrap: bool, input: &Data) -> TokenStream {
    match input {
        Data::Struct(st) => match &st.fields {
            Fields::Named(fields) => {
                let fields = fields
                    .named
                    .iter()
                    .filter(|f| {
                        !f.attrs.iter().any(|f| is_attr(f, "pack", &FIELD_ATTRS, "skip"))
                    })
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
                        !f.attrs.iter().any(|f| is_attr(f, "pack", &FIELD_ATTRS, "skip"))
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
                            !f.attrs
                                .iter()
                                .any(|a| is_attr(a, "pack", &FIELD_ATTRS, "skip"))
                        })
                        .map(|f| &f.ident);
                    let size_fields = f
                        .named
                        .iter()
                        .filter(|f| {
                            !f.attrs
                                .iter()
                                .any(|a| is_attr(a, "pack", &FIELD_ATTRS, "skip"))
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
                        let skip = f
                            .attrs
                            .iter()
                            .any(|a| is_attr(a, "pack", &FIELD_ATTRS, "skip"));
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
                            !f.attrs
                                .iter()
                                .any(|a| is_attr(a, "pack", &FIELD_ATTRS, "skip"))
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
                    .filter(|f| {
                        !f.attrs.iter().any(|a| is_attr(a, "pack", &FIELD_ATTRS, "skip"))
                    })
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
                        !f.attrs.iter().any(|a| is_attr(a, "pack", &FIELD_ATTRS, "skip"))
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
                    .unwrap_or_else(|| {
                        if tagged.is_empty() {
                            Index::from(i).to_token_stream()
              		    } else {
             			    panic!("all cases must be tagged or none must be")
              		    }
                    });
                match &v.fields {
                    Fields::Named(f) => {
             			let match_fields = f
                            .named
                            .iter()
                            .filter(|f| !f.attrs.iter().any(|a| is_attr(a, "pack", &FIELD_ATTRS, "skip")))
                            .map(|f| &f.ident);
             			let pack_fields = f
                            .named
                            .iter()
                            .filter(|f| !f.attrs.iter().any(|a| is_attr(a, "pack", &FIELD_ATTRS, "skip")))
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
                        let match_fields = f.unnamed
                            .iter()
                            .enumerate()
                            .map(|(i, f)| {
                                if f.attrs.iter().any(|a| is_attr(a, "pack", &FIELD_ATTRS, "skip")) {
                                    format_ident!("_")
                                } else {
                    				format_ident!("field{}", i)
                                }
                            });
                        let pack_fields = f
                            .unnamed
                            .iter()
                            .enumerate()
                            .filter(|(_, f)| !f.attrs.iter().any(|a| is_attr(a, "pack", &FIELD_ATTRS, "skip")))
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
    let is_skipped = f.attrs.iter().any(|a| is_attr(a, "pack", &FIELD_ATTRS, "skip"));
    let is_default = f.attrs.iter().any(|a| is_attr(a, "pack", &FIELD_ATTRS, "default"));
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
    let is_skipped = f.attrs.iter().any(|a| is_attr(a, "pack", &FIELD_ATTRS, "skip"));
    let is_default = f.attrs.iter().any(|a| is_attr(a, "pack", &FIELD_ATTRS, "default"));
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
                            if v.attrs
                                .iter()
                                .any(|a| is_attr(a, "pack", &ENUM_ATTRS, "other"))
                            {
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
                            if v.attrs
                                .iter()
                                .any(|a| is_attr(a, "pack", &ENUM_ATTRS, "other"))
                            {
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
                            if v.attrs
                                .iter()
                                .any(|a| is_attr(a, "pack", &ENUM_ATTRS, "other"))
                            {
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
    let no_wrap =
        input.attrs.iter().any(|a| is_attr(a, "pack", &["unwrapped"], "unwrapped"));
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

// --- Value derive helpers ---

fn get_value_rename(att: &Attribute) -> Option<String> {
    parse_attr(att, "value", |i, mut ts| {
        let i_str = i.to_string();
        if !VALUE_FIELD_ATTRS.contains(&i_str.as_str())
            && !VALUE_VARIANT_ATTRS.contains(&i_str.as_str())
        {
            panic!("invalid value attribute '{}'", i_str)
        }
        if i_str != "rename" {
            if ts.next().is_some() {
                panic!("'{}' attribute does not take a value", i_str);
            }
            return None;
        }
        match ts.next() {
            Some(TokenTree::Punct(p)) if p.as_char() == '=' => {}
            _ => panic!("expected rename = \"...\""),
        }
        match ts.next() {
            Some(TokenTree::Literal(lit)) => {
                let s = lit.to_string();
                Some(s.trim_matches('"').to_owned())
            }
            _ => panic!("expected string literal after rename ="),
        }
    })
    .flatten()
}

fn is_value_attr(att: &Attribute, s: &str) -> bool {
    is_attr(att, "value", &VALUE_FIELD_ATTRS, s)
}

fn value_field_name(f: &Field) -> String {
    f.attrs
        .iter()
        .find_map(get_value_rename)
        .unwrap_or_else(|| f.ident.as_ref().unwrap().to_string())
}

fn value_variant_name(v: &syn::Variant) -> String {
    v.attrs.iter().find_map(get_value_rename).unwrap_or_else(|| v.ident.to_string())
}

fn is_pure_unit_enum(data: &syn::DataEnum) -> bool {
    data.variants.iter().all(|v| matches!(v.fields, Fields::Unit))
}

fn check_no_tuple_field_attrs(fields: &syn::FieldsUnnamed) {
    for (i, f) in fields.unnamed.iter().enumerate() {
        if f.attrs.iter().any(|a| is_value_attr(a, "skip")) {
            panic!("#[value(skip)] is not supported on tuple fields (field {})", i);
        }
        if f.attrs.iter().any(|a| get_value_rename(a).is_some()) {
            panic!("#[value(rename)] is not supported on tuple fields (field {})", i);
        }
    }
}

/// Collect non-skipped named fields sorted alphabetically by wire name.
/// Returns (wire_name, field_ident, is_default) triples.
fn sorted_named_fields(fields: &syn::FieldsNamed) -> Vec<(String, Ident, bool)> {
    let mut triples: Vec<(String, Ident, bool)> = fields
        .named
        .iter()
        .filter(|f| !f.attrs.iter().any(|a| is_value_attr(a, "skip")))
        .map(|f| {
            let is_default = f.attrs.iter().any(|a| is_value_attr(a, "default"));
            (value_field_name(f), f.ident.clone().unwrap(), is_default)
        })
        .collect();
    triples.sort_by(|a, b| a.0.cmp(&b.0));
    triples
}

fn into_value_named_struct(fields: &syn::FieldsNamed) -> TokenStream {
    let sorted = sorted_named_fields(fields);
    let entries = sorted.iter().map(|(name, ident, _)| {
        quote! {
            (netidx_value::arcstr::literal!(#name), Into::<netidx_value::Value>::into(v.#ident))
        }
    });
    let n = sorted.len();
    quote! {
        let arr: [(netidx_value::arcstr::ArcStr, netidx_value::Value); #n] = [#(#entries),*];
        arr.into()
    }
}

fn into_value_unnamed_struct(fields: &syn::FieldsUnnamed) -> TokenStream {
    check_no_tuple_field_attrs(fields);
    let entries = fields.unnamed.iter().enumerate().map(|(i, _)| {
        let idx = Index::from(i);
        quote! { Into::<netidx_value::Value>::into(v.#idx) }
    });
    let n = fields.unnamed.len();
    quote! {
        let arr: [netidx_value::Value; #n] = [#(#entries),*];
        arr.into()
    }
}

fn into_value_body(type_name: &Ident, data: &Data) -> TokenStream {
    match data {
        Data::Struct(st) => match &st.fields {
            Fields::Named(fields) => into_value_named_struct(fields),
            Fields::Unnamed(fields) => into_value_unnamed_struct(fields),
            Fields::Unit => panic!("unit structs are not supported by IntoValue"),
        },
        Data::Enum(en) => {
            if is_pure_unit_enum(en) {
                let arms = en.variants.iter().map(|v| {
                    let ident = &v.ident;
                    let vname = value_variant_name(v);
                    quote! { #type_name::#ident => netidx_value::Value::from(#vname) }
                });
                quote! { match v { #(#arms),* } }
            } else {
                let arms = en.variants.iter().map(|v| {
                    let ident = &v.ident;
                    let vname = value_variant_name(v);
                    match &v.fields {
                        Fields::Unit => quote! {
                            #type_name::#ident => netidx_value::Value::String(
                                netidx_value::arcstr::literal!(#vname)
                            )
                        },
                        Fields::Named(fields) => {
                            let sorted = sorted_named_fields(fields);
                            let match_idents = sorted.iter().map(|(_, fident, _)| fident);
                            let entries = sorted.iter().map(|(wire, fident, _)| {
                                quote! {
                                    (netidx_value::arcstr::literal!(#wire),
                                     Into::<netidx_value::Value>::into(#fident))
                                }
                            });
                            let n = sorted.len();
                            quote! {
                                #type_name::#ident { #(#match_idents),*, .. } => {
                                    let __arr: [(netidx_value::arcstr::ArcStr, netidx_value::Value); #n] =
                                        [#(#entries),*];
                                    (
                                        netidx_value::arcstr::literal!(#vname),
                                        netidx_value::Value::from(__arr),
                                    ).into()
                                }
                            }
                        }
                        Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                            check_no_tuple_field_attrs(fields);
                            quote! {
                                #type_name::#ident(f0) => (
                                    netidx_value::arcstr::literal!(#vname),
                                    Into::<netidx_value::Value>::into(f0),
                                ).into()
                            }
                        }
                        Fields::Unnamed(fields) => {
                            check_no_tuple_field_attrs(fields);
                            let bindings: Vec<_> = fields
                                .unnamed
                                .iter()
                                .enumerate()
                                .map(|(i, _)| format_ident!("f{}", i))
                                .collect();
                            let entries = bindings.iter().map(|b| {
                                quote! { Into::<netidx_value::Value>::into(#b) }
                            });
                            let total = fields.unnamed.len() + 1; // tag + args
                            quote! {
                                #type_name::#ident(#(#bindings),*) => {
                                    let __arr: [netidx_value::Value; #total] = [
                                        netidx_value::Value::String(
                                            netidx_value::arcstr::literal!(#vname)
                                        ),
                                        #(#entries),*
                                    ];
                                    __arr.into()
                                }
                            }
                        }
                    }
                });
                quote! { match v { #(#arms),* } }
            }
        }
        Data::Union(_) => panic!("unions are not supported by IntoValue"),
    }
}

/// Generate merge-join extraction code for named fields.
/// Returns (field_extraction_stmts, construct_field_exprs, defaulted_field_exprs).
/// Caller must set up `__iter` as a peekable iterator over sorted (ArcStr, Value) pairs.
fn from_value_named_field_extraction(
    fields: &syn::FieldsNamed,
) -> (Vec<TokenStream>, Vec<TokenStream>, Vec<TokenStream>) {
    let sorted = sorted_named_fields(fields);
    let extractions: Vec<_> = sorted
        .iter()
        .enumerate()
        .map(|(i, (wire, _, is_default))| {
            let binding = format_ident!("__field_{}", i);
            let is_default = *is_default;
            let missing_arm = if is_default {
                quote! { break std::default::Default::default() }
            } else {
                quote! { anyhow::bail!("missing field '{}'", #wire) }
            };
            quote! {
                let #binding = loop {
                    match __iter.peek() {
                        Some((k, _)) if k.as_str() < #wire => { __iter.next(); }
                        Some((k, _)) if k.as_str() == #wire => {
                            break netidx_value::FromValue::from_value(
                                __iter.next().unwrap().1,
                            )?;
                        }
                        _ => { #missing_arm }
                    }
                };
            }
        })
        .collect();
    let construct_fields: Vec<_> = sorted
        .iter()
        .enumerate()
        .map(|(i, (_, fident, _))| {
            let binding = format_ident!("__field_{}", i);
            quote! { #fident: #binding }
        })
        .collect();
    let defaulted: Vec<_> = fields
        .named
        .iter()
        .filter(|f| f.attrs.iter().any(|a| is_value_attr(a, "skip")))
        .map(|f| {
            let fident = f.ident.as_ref().unwrap();
            quote! { #fident: std::default::Default::default() }
        })
        .collect();
    (extractions, construct_fields, defaulted)
}

fn from_value_named_struct(name: &Ident, fields: &syn::FieldsNamed) -> TokenStream {
    let (extractions, construct_fields, defaulted) =
        from_value_named_field_extraction(fields);
    quote! {
        let mut __pairs: Vec<(netidx_value::arcstr::ArcStr, netidx_value::Value)> =
            v.cast_to()?;
        __pairs.sort_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
        let mut __iter = __pairs.into_iter().peekable();
        #(#extractions)*
        Ok(#name { #(#construct_fields,)* #(#defaulted,)* })
    }
}

fn from_value_unnamed_struct(name: &Ident, fields: &syn::FieldsUnnamed) -> TokenStream {
    check_no_tuple_field_attrs(fields);
    let n = fields.unnamed.len();
    if n == 1 {
        let ty = &fields.unnamed[0].ty;
        quote! {
            Ok(#name(<#ty as netidx_value::FromValue>::from_value(v)?))
        }
    } else {
        let bindings: Vec<_> = (0..n).map(|i| format_ident!("f{}", i)).collect();
        let field_types: Vec<_> = fields.unnamed.iter().map(|f| &f.ty).collect();
        let construct = bindings.iter().map(|b| quote! { #b });
        quote! {
            let (#(#bindings),*) = v.cast_to::<(#(#field_types),*)>()?;
            Ok(#name(#(#construct),*))
        }
    }
}

fn from_value_body(name: &Ident, data: &Data) -> TokenStream {
    match data {
        Data::Struct(st) => match &st.fields {
            Fields::Named(fields) => from_value_named_struct(name, fields),
            Fields::Unnamed(fields) => from_value_unnamed_struct(name, fields),
            Fields::Unit => panic!("unit structs are not supported by FromValue"),
        },
        Data::Enum(en) => {
            if is_pure_unit_enum(en) {
                let arms = en.variants.iter().map(|v| {
                    let ident = &v.ident;
                    let vname = value_variant_name(v);
                    quote! { #vname => Ok(#name::#ident) }
                });
                let enum_name = name.to_string();
                quote! {
                    match &v {
                        netidx_value::Value::String(s) => match s.as_str() {
                            #(#arms,)*
                            s => anyhow::bail!("unknown {} variant '{s}'", #enum_name),
                        },
                        _ => anyhow::bail!("expected string for {}", #enum_name),
                    }
                }
            } else {
                let enum_name = name.to_string();
                let has_unit = en.variants.iter().any(|v| matches!(v.fields, Fields::Unit));
                let unit_arms: Vec<_> = en.variants.iter()
                    .filter(|v| matches!(v.fields, Fields::Unit))
                    .map(|v| {
                        let ident = &v.ident;
                        let vname = value_variant_name(v);
                        quote! { #vname => Ok(#name::#ident) }
                    })
                    .collect();
                let non_unit_arms: Vec<_> = en.variants.iter()
                    .filter(|v| !matches!(v.fields, Fields::Unit))
                    .map(|v| {
                        let ident = &v.ident;
                        let vname = value_variant_name(v);
                        match &v.fields {
                            Fields::Unit => unreachable!(),
                            Fields::Named(fields) => {
                                let (extractions, construct_fields, defaulted) =
                                    from_value_named_field_extraction(fields);
                                quote! {
                                    #vname => {
                                        let __data = __arr.get(1).cloned()
                                            .ok_or_else(|| anyhow::anyhow!(
                                                "expected variant data for {}", #vname
                                            ))?;
                                        let mut __pairs: Vec<(
                                            netidx_value::arcstr::ArcStr,
                                            netidx_value::Value,
                                        )> = __data.cast_to()?;
                                        __pairs.sort_by(|(a, _), (b, _)| a.as_str().cmp(b.as_str()));
                                        let mut __iter = __pairs.into_iter().peekable();
                                        #(#extractions)*
                                        Ok(#name::#ident { #(#construct_fields,)* #(#defaulted,)* })
                                    }
                                }
                            }
                            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                                check_no_tuple_field_attrs(fields);
                                let ty = &fields.unnamed[0].ty;
                                quote! {
                                    #vname => {
                                        let __data = __arr.get(1).cloned()
                                            .ok_or_else(|| anyhow::anyhow!(
                                                "expected variant data for {}", #vname
                                            ))?;
                                        Ok(#name::#ident(
                                            <#ty as netidx_value::FromValue>::from_value(__data)?
                                        ))
                                    }
                                }
                            }
                            Fields::Unnamed(fields) => {
                                check_no_tuple_field_attrs(fields);
                                let field_conversions: Vec<_> = fields.unnamed.iter()
                                    .enumerate()
                                    .map(|(i, f)| {
                                        let binding = format_ident!("f{}", i);
                                        let ty = &f.ty;
                                        let idx = i + 1; // skip tag at index 0
                                        quote! {
                                            let #binding = <#ty as netidx_value::FromValue>::from_value(
                                                __arr.get(#idx).cloned()
                                                    .ok_or_else(|| anyhow::anyhow!(
                                                        "missing tuple field {} for variant {}",
                                                        #i, #vname
                                                    ))?
                                            )?;
                                        }
                                    })
                                    .collect();
                                let bindings: Vec<_> = (0..fields.unnamed.len())
                                    .map(|i| format_ident!("f{}", i))
                                    .collect();
                                quote! {
                                    #vname => {
                                        #(#field_conversions)*
                                        Ok(#name::#ident(#(#bindings),*))
                                    }
                                }
                            }
                        }
                    })
                    .collect();
                let string_check = if has_unit {
                    quote! {
                        if let netidx_value::Value::String(ref __tag) = v {
                            return match __tag.as_str() {
                                #(#unit_arms,)*
                                s => anyhow::bail!("unknown {} variant '{s}'", #enum_name),
                            };
                        }
                    }
                } else {
                    quote! {}
                };
                quote! {
                    #string_check
                    let __arr: Vec<netidx_value::Value> = v.cast_to()?;
                    if __arr.is_empty() {
                        anyhow::bail!("empty variant array for {}", #enum_name);
                    }
                    let __tag = match &__arr[0] {
                        netidx_value::Value::String(s) => s.clone(),
                        _ => anyhow::bail!("expected string tag for {}", #enum_name),
                    };
                    match __tag.as_str() {
                        #(#non_unit_arms,)*
                        s => anyhow::bail!("unknown {} variant '{s}'", #enum_name),
                    }
                }
            }
        }
        Data::Union(_) => panic!("unions are not supported by FromValue"),
    }
}

#[proc_macro_derive(IntoValue, attributes(value))]
pub fn derive_into_value(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    for param in &mut input.generics.params {
        if let GenericParam::Type(typ) = param {
            typ.bounds.push(parse_quote!(Into<netidx_value::Value>))
        }
    }
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let body = into_value_body(&name, &input.data);
    let expanded = quote! {
        impl #impl_generics From<#name #ty_generics> for netidx_value::Value #where_clause {
            fn from(v: #name #ty_generics) -> netidx_value::Value {
                #body
            }
        }
    };
    proc_macro::TokenStream::from(expanded)
}

#[proc_macro_derive(FromValue, attributes(value))]
pub fn derive_from_value(input: proc_macro::TokenStream) -> proc_macro::TokenStream {
    let mut input = parse_macro_input!(input as DeriveInput);
    let name = input.ident;
    for param in &mut input.generics.params {
        if let GenericParam::Type(typ) = param {
            typ.bounds.push(parse_quote!(netidx_value::FromValue))
        }
    }
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();
    let body = from_value_body(&name, &input.data);
    let expanded = quote! {
        impl #impl_generics netidx_value::FromValue for #name #ty_generics #where_clause {
            fn from_value(v: netidx_value::Value) -> anyhow::Result<Self> {
                #body
            }
        }
    };
    proc_macro::TokenStream::from(expanded)
}
