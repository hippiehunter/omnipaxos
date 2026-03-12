use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, DeriveInput};

/// Derive macro for declaring an [OmniPaxos](https://crates.io/crates/omnipaxos) log entry type.
///
/// ## Usage
///
/// ```ignore
/// #[derive(Clone, Debug, Entry)]
/// #[snapshot(KVSnapshot)] // KVSnapshot is a type that implements the Snapshot trait. Remove this if snapshot is not used.
/// pub struct KeyValue {
///     pub key: String,
///     pub value: u64,
/// }
/// ```
#[proc_macro_derive(Entry, attributes(snapshot))]
pub fn entry_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let ast = parse_macro_input!(input as DeriveInput);

    // Get the name of the struct we're deriving Entry for
    let name = &ast.ident;
    let snapshot_type = get_snapshot_type(&ast);
    // Generate the implementation of Entry using the quote! macro
    let gen = quote! {
        impl ::omnipaxos::storage::Entry for #name
        {
            type Snapshot = #snapshot_type;
        }
    };

    // Convert the generated code back into tokens and return them
    gen.into()
}

fn get_snapshot_type(ast: &DeriveInput) -> quote::__private::TokenStream {
    let snapshot_type = ast
        .attrs
        .iter()
        .find_map(|attr| {
            if attr.path().is_ident("snapshot") {
                let t = attr.parse_args::<syn::Type>().expect("Expected type");
                Some(quote!(#t))
            } else {
                None
            }
        })
        .unwrap_or_else(|| quote!(::omnipaxos::storage::NoSnapshot));
    snapshot_type
}
