use std::collections::HashSet;

use proc_macro2::{Delimiter, TokenStream, TokenTree, token_stream};
use quote::{ToTokens, format_ident, quote};
use syn::{
    AttrStyle, Attribute, Data, DeriveInput, Field, Fields, GenericParam, Ident, Index,
    parse_macro_input, parse_quote,
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

/// The shape a type's derives write it in: `#[value(shape = "path")]`
/// on the type, else netidx's own.
fn value_shape(attrs: &[Attribute]) -> TokenStream {
    for att in attrs.iter().filter(|a| a.path().is_ident("value")) {
        let mut shape = None;
        att.parse_nested_meta(|meta| {
            if meta.path.is_ident("shape") {
                let lit: syn::LitStr = meta.value()?.parse()?;
                shape = Some(lit.parse::<syn::Path>()?);
                Ok(())
            } else {
                Err(meta.error("the only value attribute on a type is shape"))
            }
        })
        .unwrap_or_else(|e| panic!("{e}"));
        if let Some(shape) = shape {
            return quote! { #shape };
        }
    }
    quote! { netidx_value::NetidxShape }
}

fn into_value_named_struct(
    shape: &TokenStream,
    fields: &syn::FieldsNamed,
) -> TokenStream {
    let sorted = sorted_named_fields(fields);
    let entries = sorted.iter().map(|(name, ident, _)| {
        quote! {
            (netidx_value::arcstr::literal!(#name), Into::<netidx_value::Value>::into(v.#ident))
        }
    });
    quote! {
        <#shape as netidx_value::Shape>::make_struct([#(#entries),*])
    }
}

fn into_value_unnamed_struct(
    shape: &TokenStream,
    fields: &syn::FieldsUnnamed,
) -> TokenStream {
    check_no_tuple_field_attrs(fields);
    let entries = fields.unnamed.iter().enumerate().map(|(i, _)| {
        let idx = Index::from(i);
        quote! { Into::<netidx_value::Value>::into(v.#idx) }
    });
    if fields.unnamed.len() == 1 {
        return quote! { #(#entries)* };
    }
    quote! {
        <#shape as netidx_value::Shape>::make_tuple([#(#entries),*])
    }
}

fn into_value_body(shape: &TokenStream, type_name: &Ident, data: &Data) -> TokenStream {
    let variant = |vname: &str, payload: TokenStream| {
        quote! {
            <#shape as netidx_value::Shape>::make_variant(
                netidx_value::arcstr::literal!(#vname),
                #payload,
            )
        }
    };
    match data {
        Data::Struct(st) => match &st.fields {
            Fields::Named(fields) => into_value_named_struct(shape, fields),
            Fields::Unnamed(fields) => into_value_unnamed_struct(shape, fields),
            Fields::Unit => panic!("unit structs are not supported by IntoValue"),
        },
        Data::Enum(en) => {
            let arms = en.variants.iter().map(|v| {
                let ident = &v.ident;
                let vname = value_variant_name(v);
                match &v.fields {
                    Fields::Unit => {
                        let body = variant(
                            &vname,
                            quote! { std::iter::empty::<netidx_value::Value>() },
                        );
                        quote! { #type_name::#ident => #body }
                    }
                    Fields::Named(fields) => {
                        let sorted = sorted_named_fields(fields);
                        let match_idents = sorted.iter().map(|(_, fident, _)| fident);
                        let entries = sorted.iter().map(|(wire, fident, _)| {
                            quote! {
                                (netidx_value::arcstr::literal!(#wire),
                                 Into::<netidx_value::Value>::into(#fident))
                            }
                        });
                        let body = variant(
                            &vname,
                            quote! {
                                [<#shape as netidx_value::Shape>::make_struct([#(#entries),*])]
                            },
                        );
                        quote! { #type_name::#ident { #(#match_idents),*, .. } => #body }
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
                        let body = variant(&vname, quote! { [#(#entries),*] });
                        quote! { #type_name::#ident(#(#bindings),*) => #body }
                    }
                }
            });
            quote! { match v { #(#arms),* } }
        }
        Data::Union(_) => panic!("unions are not supported by IntoValue"),
    }
}

/// Decode the named fields of `ctor` from the struct value `src`.
fn from_value_named_fields(
    shape: &TokenStream,
    ctor: TokenStream,
    fields: &syn::FieldsNamed,
    src: TokenStream,
) -> TokenStream {
    let sorted = sorted_named_fields(fields);
    let extractions = sorted.iter().map(|(wire, fident, is_default)| {
        let missing = if *is_default {
            quote! { std::default::Default::default() }
        } else {
            quote! { anyhow::bail!("missing field '{}'", #wire) }
        };
        quote! {
            let #fident = match netidx_value::derive_struct_field(
                __fields, &mut __cursor, #wire
            )? {
                Some(v) => netidx_value::FromValue::from_value(v.clone())?,
                None => #missing,
            };
        }
    });
    let construct = sorted.iter().map(|(_, fident, _)| fident);
    let skipped = fields
        .named
        .iter()
        .filter(|f| f.attrs.iter().any(|a| is_value_attr(a, "skip")))
        .map(|f| {
            let fident = f.ident.as_ref().unwrap();
            quote! { #fident: std::default::Default::default() }
        });
    quote! {
        let __src: &netidx_value::Value = #src;
        let __fields = <#shape as netidx_value::Shape>::struct_fields(__src)
            .ok_or_else(|| anyhow::anyhow!("expected a struct, got {}", __src))?;
        let mut __cursor = 0usize;
        #(#extractions)*
        Ok(#ctor { #(#construct,)* #(#skipped,)* })
    }
}

/// Decode the positional `fields` of `ctor` from the values `src`.
fn from_value_positional(
    ctor: TokenStream,
    fields: &syn::FieldsUnnamed,
    src: TokenStream,
    what: &str,
) -> TokenStream {
    check_no_tuple_field_attrs(fields);
    let n = fields.unnamed.len();
    let bindings: Vec<_> = (0..n).map(|i| format_ident!("f{}", i)).collect();
    let conversions = fields.unnamed.iter().enumerate().map(|(i, f)| {
        let binding = &bindings[i];
        let ty = &f.ty;
        quote! {
            let #binding = <#ty as netidx_value::FromValue>::from_value(__elems[#i].clone())?;
        }
    });
    quote! {
        let __elems: &[netidx_value::Value] = #src;
        if __elems.len() != #n {
            anyhow::bail!("expected {} values for {}, got {}", #n, #what, __elems.len());
        }
        #(#conversions)*
        Ok(#ctor(#(#bindings),*))
    }
}

fn from_value_body(shape: &TokenStream, name: &Ident, data: &Data) -> TokenStream {
    match data {
        Data::Struct(st) => match &st.fields {
            Fields::Named(fields) => {
                from_value_named_fields(shape, quote! { #name }, fields, quote! { &v })
            }
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                check_no_tuple_field_attrs(fields);
                let ty = &fields.unnamed[0].ty;
                quote! {
                    Ok(#name(<#ty as netidx_value::FromValue>::from_value(v)?))
                }
            }
            Fields::Unnamed(fields) => {
                let what = name.to_string();
                let body = from_value_positional(
                    quote! { #name },
                    fields,
                    quote! { __elems },
                    &what,
                );
                quote! {
                    let __elems = <#shape as netidx_value::Shape>::tuple_elems(&v)
                        .ok_or_else(|| anyhow::anyhow!("expected a tuple for {}", #what))?;
                    #body
                }
            }
            Fields::Unit => panic!("unit structs are not supported by FromValue"),
        },
        Data::Enum(en) => {
            let enum_name = name.to_string();
            let arms = en.variants.iter().map(|v| {
                let ident = &v.ident;
                let vname = value_variant_name(v);
                match &v.fields {
                    Fields::Unit => quote! {
                        (#vname, []) => Ok(#name::#ident)
                    },
                    Fields::Named(fields) => {
                        let body = from_value_named_fields(
                            shape,
                            quote! { #name::#ident },
                            fields,
                            quote! { __data },
                        );
                        quote! { (#vname, [__data]) => { #body } }
                    }
                    Fields::Unnamed(fields) => {
                        let what = format!("{enum_name}::{vname}");
                        let body = from_value_positional(
                            quote! { #name::#ident },
                            fields,
                            quote! { __payload },
                            &what,
                        );
                        quote! { (#vname, __payload) if !__payload.is_empty() => { #body } }
                    }
                }
            });
            quote! {
                let (__tag, __payload) = <#shape as netidx_value::Shape>::variant(&v)
                    .ok_or_else(|| anyhow::anyhow!("expected a {} variant, got {}", #enum_name, v))?;
                match (__tag.as_str(), __payload) {
                    #(#arms,)*
                    (s, _) => anyhow::bail!("unknown {} variant '{s}'", #enum_name),
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
    let shape = value_shape(&input.attrs);
    let body = into_value_body(&shape, &name, &input.data);
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
    let shape = value_shape(&input.attrs);
    let body = from_value_body(&shape, &name, &input.data);
    let expanded = quote! {
        impl #impl_generics netidx_value::FromValue for #name #ty_generics #where_clause {
            fn from_value(v: netidx_value::Value) -> anyhow::Result<Self> {
                #body
            }
        }
    };
    proc_macro::TokenStream::from(expanded)
}
