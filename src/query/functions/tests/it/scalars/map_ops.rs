// Copyright 2022 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::io::Write;

use databend_common_expression::types::*;
use databend_common_expression::FromData;
use goldenfile::Mint;

use super::run_ast;

#[test]
fn test_map_ops() {
    log::info!("shamb0, launch test_map_ops()");

    let mut mint = Mint::new("tests/it/scalars/testdata");
    let file = &mut mint.new_goldenfile("map_ops.txt").unwrap();

    test_map_cat_empty_maps(file);
    test_map_cat_ops_basic(file);
    test_map_cat_deduplicate(file);
    test_map_cat_different_sizes(file);
    test_map_cat_null_values(file);
    test_map_cat_nested_maps(file);
}

fn test_map_cat_empty_maps(file: &mut impl Write) {
    run_ast(file, "map_cat({}, {})", &[]);
    run_ast(file, "map_cat({}, {'k1': 'v1'})", &[]);
    run_ast(file, "map_cat({'k1': 'v1'}, {})", &[]);
}

fn test_map_cat_ops_basic(file: &mut impl Write) {
    let columns = [
        ("a_col", StringType::from_data(vec!["a_k1", "a_k2", "a_k3"])),
        ("b_col", StringType::from_data(vec!["b_k1", "b_k2", "b_k3"])),
        ("c_col", StringType::from_data(vec!["c_k1", "c_k2", "c_k3"])),
        ("d_col", StringType::from_data(vec!["aaa1", "aaa2", "aaa3"])),
        ("e_col", StringType::from_data(vec!["bbb1", "bbb2", "bbb3"])),
        ("f_col", StringType::from_data(vec!["ccc1", "ccc2", "ccc3"])),
    ];

    run_ast(
        file,
        "map_cat(map([a_col, b_col], [d_col, e_col]), map([c_col], [f_col]))",
        &columns,
    );
}

fn test_map_cat_deduplicate(file: &mut impl Write) {
    run_ast(file, "map_cat({'k1':'v1','k2':'v2'}, {'k1':'abc'})", &[]);

    let columns = [
        ("a_col", StringType::from_data(vec!["a_k1", "a_k2", "c_k3"])),
        ("b_col", StringType::from_data(vec!["b_k1", "c_k2", "b_k3"])),
        ("c_col", StringType::from_data(vec!["c_k1", "c_k2", "c_k3"])),
        ("d_col", StringType::from_data(vec!["aaa1", "aaa2", "aaa3"])),
        ("e_col", StringType::from_data(vec!["bbb1", "bbb2", "bbb3"])),
        ("f_col", StringType::from_data(vec!["ccc1", "ccc2", "ccc3"])),
    ];

    run_ast(
        file,
        "map_cat(map([a_col, b_col], [d_col, e_col]), map([c_col], [f_col]))",
        &columns,
    );
}

fn test_map_cat_different_sizes(file: &mut impl Write) {
    run_ast(file, "map_cat({'k1': 'v1', 'k2': 'v2'}, {'k3': 'v3'})", &[]);
    run_ast(file, "map_cat({'k1': 'v1'}, {'k2': 'v2', 'k3': 'v3'})", &[]);
}

fn test_map_cat_null_values(file: &mut impl Write) {
    run_ast(
        file,
        "map_cat({'k1': 'v1', 'k2': NULL}, {'k2': 'v2', 'k3': NULL})",
        &[],
    );
}

fn test_map_cat_nested_maps(file: &mut impl Write) {
    run_ast(
        file,
        "map_cat({'k1': {'nk1': 'nv1'}, 'k2': {'nk2': 'nv2'}}, {'k2': {'nk3': 'nv3'}, 'k3': {'nk4': 'nv4'}})",
        &[],
    );
    run_ast(
        file,
        "map_cat({'k1': {'nk1': 'nv1'}, 'k2': {'nk2': 'nv2'}}, {'k1': {'nk1': 'new_nv1'}, 'k2': {'nk3': 'nv3'}})",
        &[],
    );
}

// fn test_map_cat_different_value_types(file: &mut impl Write) {
//     run_ast(file, "map_cat({'k1': 'v1', 'k2': 123}, {'k2': 456, 'k3': true})", &[]);
//     run_ast(file, "map_cat({'k1': 'v1', 'k2': 3.14}, {'k2': 2.718, 'k3': 'pi'})", &[]);
//     run_ast(file, "map_cat({'k1': true, 'k2': false}, {'k2': true, 'k3': NULL})", &[]);
//     run_ast(file, "map_cat({'k1': 'v1', 'k2': 42}, {'k2': DATE '2023-05-08', 'k3': TIMESTAMP '2023-05-08 10:30:00'})", &[]);
//     run_ast(file, "map_cat({'k1': 'v1', 'k2': ARRAY['a', 'b', 'c']}, {'k2': ARRAY[1, 2, 3], 'k3': MAP(ARRAY['x', 'y'], ARRAY[10, 20])})", &[]);
// }
