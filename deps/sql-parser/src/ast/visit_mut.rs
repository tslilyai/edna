// Copyright Materialize, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Traversal of a mutable AST.
//!
//! This module provides a [`VisitMut`] trait that is like the [`Visit`] trait
//! but operates on a mutable borrow, rather than an immutable borrow, of the
//! syntax tree.
//!
//! Each method of the [`VisitMut`] trait is a hook that can be overridden to
//! customize the behavior when visiting the corresponding type of node. By
//! default, every method recursively visits the substructure of the input
//! by invoking the right visitor method of each of its fields.
//!
//! ```
//! # use sql_parser::ast::{Expr, Function, FunctionArgs, ObjectName, WindowSpec};
//! #
//! pub trait VisitMut<'ast> {
//!     /* ... */
//!
//!     fn visit_function_mut(&mut self, node: &'ast mut Function) {
//!         visit_function_mut(self, node);
//!     }
//!
//!     /* ... */
//!     # fn visit_object_name_mut(&mut self, node: &'ast mut ObjectName);
//!     # fn visit_function_args_mut(&mut self, node: &'ast mut FunctionArgs);
//!     # fn visit_expr_mut(&mut self, node: &'ast mut Expr);
//!     # fn visit_window_spec_mut(&mut self, node: &'ast mut WindowSpec);
//! }
//!
//! pub fn visit_function_mut<'ast, V>(visitor: &mut V, node: &'ast mut Function)
//! where
//!     V: VisitMut<'ast> + ?Sized,
//! {
//!     visitor.visit_object_name_mut(&mut node.name);
//!     visitor.visit_function_args_mut(&mut node.args);
//!     if let Some(filter) = &mut node.filter {
//!         visitor.visit_expr_mut(&mut *filter);
//!     }
//!     if let Some(over) = &mut node.over {
//!         visitor.visit_window_spec_mut(over);
//!     }
//! }
//! ```
//!
//! [`Visit`]: super::visit::Visit
//!
//! # Examples
//!
//! This visitor removes parentheses from expressions.
//!
//! ```
//! use std::error::Error;
//!
//! use sql_parser::ast::Expr;
//! use sql_parser::ast::visit_mut::{self, VisitMut};
//!
//! struct RemoveParens;
//!
//! impl<'a> VisitMut<'a> for RemoveParens {
//!     fn visit_expr_mut(&mut self, expr: &'a mut Expr) {
//!         visit_mut::visit_expr_mut(self, expr);
//!         if let Expr::Nested(e) = expr {
//!             *expr = (**e).clone();
//!         }
//!     }
//! }
//!
//! fn main() -> Result<(), Box<dyn Error>> {
//!     let sql = "(a + ((b))) + c";
//!     let mut expr = sql_parser::parser::parse_expr(sql.into())?;
//!     RemoveParens.visit_expr_mut(&mut expr);
//!     let expected = sql_parser::parser::parse_expr("a + b + c".into())?;
//!     assert_eq!(expr, expected);
//!     Ok(())
//! }
//! ```
//!
//! # Implementation notes
//!
//! This module is automatically generated by the crate's build script. Changes
//! to the AST will be automatically propagated to the visitor.
//!
//! This approach to AST visitors is inspired by the [`syn`] crate. These
//! module docs are directly derived from the [`syn::visit_mut`] module docs.
//!
//! [`syn`]: https://docs.rs/syn/1.*/syn/index.html
//! [`syn::visit_mut`]: https://docs.rs/syn/1.*/syn/visit_mut/index.html

#![allow(clippy::all)]
#![allow(unused_variables)]

//use super::*;

//include!(concat!(env!("OUT_DIR"), "/visit_mut.rs"));
