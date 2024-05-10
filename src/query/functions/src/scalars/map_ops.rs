// Copyright 2021 Datafuse Labs
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
use std::collections::HashSet;

use databend_common_expression::types::map::KvColumnBuilder;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::ArrayType;
use databend_common_expression::types::EmptyMapType;
use databend_common_expression::types::GenericType;
use databend_common_expression::types::MapType;
use databend_common_expression::types::NullableType;
use databend_common_expression::vectorize_with_builder_2_arg;
use databend_common_expression::FunctionDomain;
use databend_common_expression::FunctionRegistry;
use databend_common_expression::Value;

pub fn register(registry: &mut FunctionRegistry) {
    registry.register_2_arg_core::<NullableType<EmptyMapType>, NullableType<EmptyMapType>, EmptyMapType, _, _>(
        "map_cat",
        |_, _, _| FunctionDomain::Full,
        |_, _, _| Value::Scalar(()),
    );

    // registry.register_passthrough_nullable_2_arg(
    //     "map_cat_wip",
    //     |_, _, _| FunctionDomain::MayThrow,
    //     vectorize_with_builder_2_arg::<
    //         MapType<GenericType<0>, GenericType<1>>,
    //         MapType<GenericType<0>, GenericType<1>>,
    //         MapType<GenericType<0>, GenericType<1>>,
    //     >(|lhs, rhs, output_map, _| {

    //         log::info!("lhs :: {:#?}", lhs);
    //         output_map.append_column(&KvColumnBuilder::from_column(lhs));

    //         log::info!("rhs :: {:#?}", rhs);
    //         output_map.append_column(&KvColumnBuilder::from_column(rhs));

    //         log::info!("output_map :: {:#?}", output_map);

    //         output_map.commit_row();
    //     }),
    // );

    registry.register_passthrough_nullable_2_arg(
        "map_cat",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<
            MapType<GenericType<0>, GenericType<1>>,
            MapType<GenericType<0>, GenericType<1>>,
            MapType<GenericType<0>, GenericType<1>>,
        >(|lhs, rhs, output_map, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output_map.len()) {
                    output_map.push_default();
                    return;
                }
            }

            let mut concatenated_map_builder =
                ArrayType::create_builder(lhs.len() + rhs.len(), ctx.generics);
            let mut detect_dup_keys = HashSet::new();

            for (lhs_key, lhs_value) in lhs.iter() {
                if let Some((_, rhs_value)) = rhs.iter().find(|(rhs_key, _)| lhs_key == *rhs_key) {
                    detect_dup_keys.insert(lhs_key.clone());
                    concatenated_map_builder.put_item((lhs_key.clone(), rhs_value.clone()));
                } else {
                    concatenated_map_builder.put_item((lhs_key.clone(), lhs_value.clone()));
                }
            }

            for (rhs_key, rhs_value) in rhs.iter() {
                if !detect_dup_keys.contains(&rhs_key) {
                    concatenated_map_builder.put_item((rhs_key, rhs_value));
                }
            }

            log::info!("cmb :: {:#?}", concatenated_map_builder);

            concatenated_map_builder.commit_row();

            log::info!("cmb :: {:#?}", concatenated_map_builder);

            output_map.append_column(&concatenated_map_builder.build());

            log::info!("output_map :: {:#?}", output_map);
        }),
    );

    // registry.register_passthrough_nullable_2_arg(
    //     "map_cat_impl1",
    //     |_, _, _| FunctionDomain::MayThrow,
    //     vectorize_with_builder_2_arg::<
    //         MapType<GenericType<0>, GenericType<1>>,
    //         MapType<GenericType<0>, GenericType<1>>,
    //         MapType<GenericType<0>, GenericType<1>>,
    //     >(|lhs, rhs, output_map, ctx| {
    //         let capacity = lhs.len();
    //         let mut lhs_key_set: StackHashSet<u128, 16> = StackHashSet::with_capacity(capacity);

    //         log::info!("lhs :: {:#?}", lhs);
    //         lhs.iter().for_each(|(key, value)| {
    //             let mut hasher = SipHasher24::new();
    //             key.hash(&mut hasher);
    //             let hash128 = hasher.finish128();
    //             let hash_key = hash128.into();
    //             let _ = lhs_key_set.set_insert(hash_key);
    //             output_map.put_item((key, value))
    //         });

    //         log::info!("rhs :: {:#?}", rhs);

    //         rhs.iter().for_each(|(key, value)| {
    //             let mut hasher = SipHasher24::new();
    //             key.hash(&mut hasher);
    //             let hash128 = hasher.finish128();
    //             let hash_key = hash128.into();
    //             if lhs_key_set.contains(&hash_key) {
    //                 log::warn!("detected duplicate map key, replacing it with rhs map key");
    //             }
    //             output_map.put_item((key, value))
    //         });

    //         log::info!("output_map :: {:#?}", output_map);

    //         output_map.commit_row();
    //     }),
    // );

    registry.register_passthrough_nullable_2_arg(
        "map_cat_bup1",
        |_, _, _| FunctionDomain::MayThrow,
        vectorize_with_builder_2_arg::<
            MapType<GenericType<0>, GenericType<1>>,
            MapType<GenericType<0>, GenericType<1>>,
            MapType<GenericType<0>, GenericType<1>>,
        >(|input_map1, input_map2, output_map, ctx| {
            if let Some(validity) = &ctx.validity {
                if !validity.get_bit(output_map.len()) {
                    output_map.commit_row();
                    return;
                }
            }

            log::info!("*** map_cat, input_map1 ...***");
            input_map1
                .iter()
                .for_each(|(key, value)| log::info!("K :: {:#?}, V :: {:#?}", key, value));
            log::info!("{:#?}", input_map1);

            log::info!("map_cat, input_map2 ...");
            input_map2
                .iter()
                .for_each(|(key, value)| log::info!("K :: {:#?}, V :: {:#?}", key, value));
            log::info!("{:#?}", input_map2);

            let mut concatenated_map_builder = KvColumnBuilder::from_column(input_map1);
            log::info!("S1 :: {:#?}", concatenated_map_builder);

            concatenated_map_builder.append_column(&input_map2);
            log::info!("S2 :: {:#?}", concatenated_map_builder);

            let concatenated_map = KvColumnBuilder::build(concatenated_map_builder);
            log::info!("S3 :: {:#?}", concatenated_map);

            log::info!("map_cat ...");
            for (key, value) in concatenated_map.iter() {
                log::info!("K :: {:#?}, V :: {:#?}", key, value);
                output_map.put_item((key, value));
            }

            output_map.commit_row();

            log::info!("*** map_cat, Done!***");
        }),
    );
}
