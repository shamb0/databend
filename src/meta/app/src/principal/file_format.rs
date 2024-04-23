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

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::str::FromStr;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_io::constants::NULL_BYTES_ESCAPE;
use databend_common_io::escape_string;
use databend_common_io::GeometryDataType;
use paste::paste;
use serde::Deserialize;
use serde::Serialize;

use crate::principal::StageFileCompression;
use crate::principal::StageFileFormatType;

const OPT_FIELD_DELIMITER: &str = "field_delimiter";
const OPT_RECORDE_DELIMITER: &str = "record_delimiter";
const OPT_SKIP_HEADER: &str = "skip_header";
const OPT_OUTPUT_HEADER: &str = "output_header";
const OPT_NAN_DISPLAY: &str = "nan_display";
const OPT_NULL_DISPLAY: &str = "null_display";
const OPT_ESCAPE: &str = "escape";
const OPT_QUOTE: &str = "quote";
const OPT_ROW_TAG: &str = "row_tag";
const OPT_ERROR_ON_COLUMN_COUNT_MISMATCH: &str = "error_on_column_count_mismatch";
const MISSING_FIELD_AS: &str = "missing_field_as";
const NULL_FIELD_AS: &str = "null_field_as";
const NULL_IF: &str = "null_if";
const OPT_EMPTY_FIELD_AS: &str = "empty_field_as";
const OPT_BINARY_FORMAT: &str = "binary_format";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct FileFormatOptionsAst {
    pub options: BTreeMap<String, String>,
}

impl FileFormatOptionsAst {
    pub fn new(options: BTreeMap<String, String>) -> Self {
        FileFormatOptionsAst { options }
    }

    fn take_string(&mut self, key: &str, default: String) -> String {
        self.options.remove(key).unwrap_or(default)
    }

    fn take_type(&mut self) -> Result<StageFileFormatType> {
        match (self.options.remove("type"), self.options.remove("format")) {
            (Some(t), None) | (None, Some(t)) => {
                StageFileFormatType::from_str(&t).map_err(ErrorCode::IllegalFileFormat)
            }
            (Some(_), Some(_)) => Err(ErrorCode::IllegalFileFormat(
                "Invalid FILE_FORMAT options: both TYPE and FORMAT option are present. \
                Please only use the TYPE to specify the file format type. The FORMAT option is deprecated.",
            )),
            (None, None) => Err(ErrorCode::IllegalFileFormat(
                "Invalid FILE_FORMAT options: FILE_FORMAT must include at least one of the TYPE or NAME option. \
                Currently, neither is specified.",
            )),
        }
    }

    fn take_compression(&mut self) -> Result<StageFileCompression> {
        match self.options.remove("compression") {
            Some(c) => StageFileCompression::from_str(&c).map_err(ErrorCode::IllegalFileFormat),
            None => Ok(StageFileCompression::None),
        }
    }

    fn take_u64(&mut self, key: &str, default: u64) -> Result<u64> {
        match self.options.remove(key) {
            Some(v) => Ok(u64::from_str(&v)?),
            None => Ok(default),
        }
    }

    fn take_bool(&mut self, key: &str, default: bool) -> Result<bool> {
        match self.options.remove(key) {
            Some(v) => Ok(bool::from_str(&v.to_lowercase()).map_err(|_| {
                ErrorCode::IllegalFileFormat(format!(
                    "Invalid boolean value {} for option {}.",
                    v, key
                ))
            })?),
            None => Ok(default),
        }
    }
}

/// File format parameters after checking and parsing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum FileFormatParams {
    Csv(CsvFileFormatParams),
    Tsv(TsvFileFormatParams),
    NdJson(NdJsonFileFormatParams),
    Json(JsonFileFormatParams),
    Xml(XmlFileFormatParams),
    Parquet(ParquetFileFormatParams),
}

impl FileFormatParams {
    pub fn get_type(&self) -> StageFileFormatType {
        match self {
            FileFormatParams::Csv(_) => StageFileFormatType::Csv,
            FileFormatParams::Tsv(_) => StageFileFormatType::Tsv,
            FileFormatParams::NdJson(_) => StageFileFormatType::NdJson,
            FileFormatParams::Json(_) => StageFileFormatType::Json,
            FileFormatParams::Xml(_) => StageFileFormatType::Xml,
            FileFormatParams::Parquet(_) => StageFileFormatType::Parquet,
        }
    }

    pub fn default_by_type(format_type: StageFileFormatType) -> Result<Self> {
        match format_type {
            StageFileFormatType::Parquet => {
                Ok(FileFormatParams::Parquet(ParquetFileFormatParams::default()))
            }
            StageFileFormatType::Csv => Ok(FileFormatParams::Csv(CsvFileFormatParams::default())),
            StageFileFormatType::Tsv => Ok(FileFormatParams::Tsv(TsvFileFormatParams::default())),
            StageFileFormatType::NdJson => {
                Ok(FileFormatParams::NdJson(NdJsonFileFormatParams::default()))
            }
            StageFileFormatType::Json => {
                Ok(FileFormatParams::Json(JsonFileFormatParams::default()))
            }
            StageFileFormatType::Xml => Ok(FileFormatParams::Xml(XmlFileFormatParams::default())),
            _ => Err(ErrorCode::IllegalFileFormat(format!(
                "Unsupported file format type: {:?}",
                format_type
            ))),
        }
    }

    pub fn compression(&self) -> StageFileCompression {
        match self {
            FileFormatParams::Csv(v) => v.compression,
            FileFormatParams::Tsv(v) => v.compression,
            FileFormatParams::NdJson(v) => v.compression,
            FileFormatParams::Json(v) => v.compression,
            FileFormatParams::Xml(v) => v.compression,
            FileFormatParams::Parquet(_) => StageFileCompression::None,
        }
    }

    pub fn try_from_ast(ast: FileFormatOptionsAst, old: bool) -> Result<Self> {
        let mut ast = ast;
        let typ = ast.take_type()?;
        let params = match typ {
            StageFileFormatType::Xml => {
                let default = XmlFileFormatParams::default();
                let row_tag = ast.take_string(OPT_ROW_TAG, default.row_tag);
                let compression = ast.take_compression()?;
                FileFormatParams::Xml(XmlFileFormatParams {
                    compression,
                    row_tag,
                })
            }
            StageFileFormatType::Json => {
                let compression = ast.take_compression()?;
                FileFormatParams::Json(JsonFileFormatParams { compression })
            }
            StageFileFormatType::NdJson => {
                let compression = ast.take_compression()?;
                let missing_field_as = ast.options.remove(MISSING_FIELD_AS);
                let null_field_as = ast.options.remove(NULL_FIELD_AS);
                let null_if = ast.options.remove(NULL_IF);
                let null_if = match null_if {
                    None => {
                        vec![]
                    }
                    Some(s) => {
                        let values: Vec<String> = serde_json::from_str(&s).map_err(|_|
                            ErrorCode::InvalidArgument(format!(
                            "Invalid option value: NULL_IF is currently set to {s} (in JSON). The valid values are a list of strings."
                            )))?;
                        values
                    }
                };
                FileFormatParams::NdJson(NdJsonFileFormatParams::try_create(
                    compression,
                    missing_field_as.as_deref(),
                    null_field_as.as_deref(),
                    null_if,
                )?)
            }
            StageFileFormatType::Parquet => {
                let missing_field_as = ast.options.remove(MISSING_FIELD_AS);
                FileFormatParams::Parquet(ParquetFileFormatParams::try_create(
                    missing_field_as.as_deref(),
                )?)
            }
            StageFileFormatType::Csv => {
                let default = CsvFileFormatParams::default();
                let compression = ast.take_compression()?;
                let headers = ast.take_u64(OPT_SKIP_HEADER, default.headers)?;
                let field_delimiter = ast.take_string(OPT_FIELD_DELIMITER, default.field_delimiter);
                let record_delimiter =
                    ast.take_string(OPT_RECORDE_DELIMITER, default.record_delimiter);
                let nan_display = ast.take_string(OPT_NAN_DISPLAY, default.nan_display);
                let escape = ast.take_string(OPT_ESCAPE, default.escape);
                let quote = ast.take_string(OPT_QUOTE, default.quote);
                let null_display = ast.take_string(OPT_NULL_DISPLAY, default.null_display);
                let empty_field_as = ast
                    .options
                    .remove(OPT_EMPTY_FIELD_AS)
                    .map(|s| EmptyFieldAs::from_str(&s))
                    .transpose()?
                    .unwrap_or_default();
                let binary_format = ast
                    .options
                    .remove(OPT_BINARY_FORMAT)
                    .map(|s| BinaryFormat::from_str(&s))
                    .transpose()?
                    .unwrap_or_default();
                let error_on_column_count_mismatch = ast.take_bool(
                    OPT_ERROR_ON_COLUMN_COUNT_MISMATCH,
                    default.error_on_column_count_mismatch,
                )?;
                let output_header = ast.take_bool(OPT_OUTPUT_HEADER, default.output_header)?;
                FileFormatParams::Csv(CsvFileFormatParams {
                    compression,
                    headers,
                    field_delimiter,
                    record_delimiter,
                    null_display,
                    nan_display,
                    escape,
                    quote,
                    error_on_column_count_mismatch,
                    empty_field_as,
                    binary_format,
                    output_header,
                    geometry_format: std::default::Default::default(),
                })
            }
            StageFileFormatType::Tsv => {
                let default = TsvFileFormatParams::default();
                let compression = ast.take_compression()?;
                let headers = ast.take_u64(OPT_SKIP_HEADER, default.headers)?;
                let field_delimiter = ast.take_string(OPT_FIELD_DELIMITER, default.field_delimiter);
                let record_delimiter =
                    ast.take_string(OPT_RECORDE_DELIMITER, default.record_delimiter);
                let nan_display = ast.take_string(OPT_NAN_DISPLAY, default.nan_display);
                let escape = ast.take_string(OPT_ESCAPE, default.escape);
                let quote = ast.take_string(OPT_QUOTE, default.quote);
                FileFormatParams::Tsv(TsvFileFormatParams {
                    compression,
                    headers,
                    field_delimiter,
                    record_delimiter,
                    nan_display,
                    quote,
                    escape,
                })
            }
            _ => {
                return Err(ErrorCode::IllegalFileFormat(format!(
                    "Unsupported file format {typ:?}"
                )));
            }
        };
        if old {
            Ok(params)
        } else {
            params.check().map_err(|msg| {
                ErrorCode::BadArguments(format!(
                    "Invalid {} option value: {msg}",
                    params.get_type().to_string()
                ))
            })?;
            if ast.options.is_empty() {
                Ok(params)
            } else {
                Err(ErrorCode::IllegalFileFormat(format!(
                    "Unsupported options for {:?}:  {:?}",
                    typ, ast.options
                )))
            }
        }
    }

    pub fn check(&self) -> std::result::Result<(), String> {
        macro_rules! check_option {
            ($params:expr, $option_name:ident) => {{
                let v = &$params.$option_name;
                paste! { let check_fn  = [<check_$option_name>]; }
                check_fn(v).map_err(|msg| {
                    format!(
                        "{} is currently set to '{v}'. {msg}",
                        stringify!($option_name).to_ascii_uppercase(),
                    )
                })
            }};
        }

        match self {
            FileFormatParams::Tsv(p) => {
                check_option!(p, field_delimiter)?;
                check_option!(p, record_delimiter)?;
                check_option!(p, quote)?;
                check_option!(p, escape)?;
                check_option!(p, nan_display)?;
            }
            FileFormatParams::Csv(p) => {
                check_option!(p, field_delimiter)?;
                check_option!(p, record_delimiter)?;
                check_option!(p, quote)?;
                check_option!(p, escape)?;
                check_option!(p, nan_display)?;
            }
            FileFormatParams::Xml(p) => {
                check_option!(p, row_tag)?;
            }
            _ => {}
        }
        Ok(())
    }
}

impl Default for FileFormatParams {
    fn default() -> Self {
        FileFormatParams::Parquet(ParquetFileFormatParams {
            missing_field_as: NullAs::Error,
        })
    }
}

impl TryFrom<FileFormatOptionsAst> for FileFormatParams {
    type Error = ErrorCode;

    fn try_from(ast: FileFormatOptionsAst) -> Result<Self> {
        FileFormatParams::try_from_ast(ast, false)
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CsvFileFormatParams {
    pub compression: StageFileCompression,

    // basic
    pub field_delimiter: String,
    pub record_delimiter: String,
    pub escape: String,
    pub quote: String,
    pub error_on_column_count_mismatch: bool,

    // header
    pub headers: u64,
    pub output_header: bool,

    // field
    pub binary_format: BinaryFormat,
    pub null_display: String,
    pub nan_display: String,
    pub empty_field_as: EmptyFieldAs,
    pub geometry_format: GeometryDataType,
}

impl Default for CsvFileFormatParams {
    fn default() -> Self {
        CsvFileFormatParams {
            compression: StageFileCompression::None,
            headers: 0,
            field_delimiter: ",".to_string(),
            record_delimiter: "\n".to_string(),
            null_display: NULL_BYTES_ESCAPE.to_string(),
            nan_display: "NaN".to_string(),
            escape: "".to_string(),
            quote: "\"".to_string(),
            error_on_column_count_mismatch: true,
            empty_field_as: Default::default(),
            output_header: false,
            binary_format: Default::default(),
            geometry_format: GeometryDataType::default(),
        }
    }
}

impl CsvFileFormatParams {
    pub fn downcast_unchecked(params: &FileFormatParams) -> &CsvFileFormatParams {
        match params {
            FileFormatParams::Csv(p) => p,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct TsvFileFormatParams {
    pub compression: StageFileCompression,
    pub headers: u64,
    pub field_delimiter: String,
    pub record_delimiter: String,
    pub nan_display: String,
    pub escape: String,
    pub quote: String,
}

impl Default for TsvFileFormatParams {
    fn default() -> Self {
        TsvFileFormatParams {
            compression: StageFileCompression::None,
            headers: 0,
            field_delimiter: "\t".to_string(),
            record_delimiter: "\n".to_string(),
            nan_display: "nan".to_string(),
            escape: "\\".to_string(),
            quote: "\'".to_string(),
        }
    }
}

impl TsvFileFormatParams {
    pub fn downcast_unchecked(params: &FileFormatParams) -> &TsvFileFormatParams {
        match params {
            FileFormatParams::Tsv(p) => p,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct XmlFileFormatParams {
    pub compression: StageFileCompression,
    pub row_tag: String,
}

impl XmlFileFormatParams {
    pub fn downcast_unchecked(params: &FileFormatParams) -> &XmlFileFormatParams {
        match params {
            FileFormatParams::Xml(p) => p,
            _ => unreachable!(),
        }
    }
}

impl Default for XmlFileFormatParams {
    fn default() -> Self {
        XmlFileFormatParams {
            compression: StageFileCompression::None,
            row_tag: "row".to_string(),
        }
    }
}

/// used for both `missing_field_as` and `null_field_as`
/// for extensibility, it is stored as PB string in meta
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum NullAs {
    /// for `missing_field_as` only, and is default for it for safety,
    /// in case of wrong field names when creating table.
    #[default]
    Error,
    /// only valid for nullable column
    Null,
    /// defined when creating table, fallback to type default if no schema there
    FieldDefault,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum EmptyFieldAs {
    #[default]
    Null,
    String,
    FieldDefault,
}

impl FromStr for EmptyFieldAs {
    type Err = ErrorCode;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "string" => Ok(Self::String),
            "null" => Ok(Self::Null),
            "field_default" => Ok(Self::FieldDefault),
            _ => Err(ErrorCode::InvalidArgument(format!(
                "Invalid option value. EMPTY_FILED_AS is currently set to {s}. The valid values are NULL | STRING | FIELD_DEFAULT."
            ))),
        }
    }
}

impl Display for EmptyFieldAs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::FieldDefault => write!(f, "FILED_DEFAULT"),
            Self::Null => write!(f, "NULL"),
            Self::String => write!(f, "STRING"),
        }
    }
}

impl NullAs {
    fn parse(s: Option<&str>, option_name: &str, default: Self) -> Result<Self> {
        match s {
            Some(v) => v.parse::<NullAs>().map_err(|_| {
                let msg = format!("Invalid option value: {option_name} is set to {v}. The valid values are ERROR | NULL | FIELD_DEFAULT.");
                ErrorCode::InvalidArgument(msg)
            }),
            None => Ok(default),
        }
    }
}

impl FromStr for NullAs {
    type Err = ();

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "error" => Ok(NullAs::Error),
            "null" => Ok(NullAs::Null),
            "field_default" => Ok(NullAs::FieldDefault),
            _ => Err(()),
        }
    }
}

impl Display for NullAs {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            NullAs::Error => write!(f, "ERROR"),
            NullAs::Null => write!(f, "NULL"),
            NullAs::FieldDefault => write!(f, "FIELD_DEFAULT"),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, Default)]
pub enum BinaryFormat {
    #[default]
    Hex,
    Base64,
}

impl FromStr for BinaryFormat {
    type Err = ErrorCode;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "hex" => Ok(Self::Hex),
            "base64" => Ok(Self::Base64),
            _ => Err(ErrorCode::InvalidArgument(format!(
                "Invalid option value: BINARY_FORMAT is set to {s}. The valid values are HEX | BASE64."
            ))),
        }
    }
}

impl Display for BinaryFormat {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Hex => write!(f, "hex"),
            Self::Base64 => write!(f, "base64"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct JsonFileFormatParams {
    pub compression: StageFileCompression,
}

impl JsonFileFormatParams {
    pub fn downcast_unchecked(params: &FileFormatParams) -> &JsonFileFormatParams {
        match params {
            FileFormatParams::Json(p) => p,
            _ => unreachable!(),
        }
    }
}

impl Default for JsonFileFormatParams {
    fn default() -> Self {
        JsonFileFormatParams {
            compression: StageFileCompression::None,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NdJsonFileFormatParams {
    pub compression: StageFileCompression,
    pub missing_field_as: NullAs,
    pub null_field_as: NullAs,
    pub null_if: Vec<String>,
}

impl NdJsonFileFormatParams {
    pub fn try_create(
        compression: StageFileCompression,
        missing_field_as: Option<&str>,
        null_field_as: Option<&str>,
        null_if: Vec<String>,
    ) -> Result<Self> {
        let missing_field_as = NullAs::parse(missing_field_as, MISSING_FIELD_AS, NullAs::Error)?;
        let null_field_as = NullAs::parse(null_field_as, MISSING_FIELD_AS, NullAs::Null)?;
        if matches!(null_field_as, NullAs::Error) {
            return Err(ErrorCode::InvalidArgument(
                "Invalid option value: NULL_FIELD_AS is set to ERROR. The valid values are NULL | FIELD_DEFAULT.",
            ));
        }
        Ok(Self {
            compression,
            missing_field_as,
            null_field_as,
            null_if,
        })
    }
}

impl Default for NdJsonFileFormatParams {
    fn default() -> Self {
        NdJsonFileFormatParams {
            compression: StageFileCompression::None,
            missing_field_as: NullAs::Error,
            null_field_as: NullAs::FieldDefault,
            null_if: vec![],
        }
    }
}

impl NdJsonFileFormatParams {
    pub fn downcast_unchecked(params: &FileFormatParams) -> &NdJsonFileFormatParams {
        match params {
            FileFormatParams::NdJson(p) => p,
            _ => unreachable!(),
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ParquetFileFormatParams {
    pub missing_field_as: NullAs,
}

impl ParquetFileFormatParams {
    pub fn try_create(missing_field_as: Option<&str>) -> Result<Self> {
        let missing_field_as = NullAs::parse(missing_field_as, MISSING_FIELD_AS, NullAs::Error)?;
        Ok(Self { missing_field_as })
    }
}

impl Display for FileFormatParams {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            FileFormatParams::Csv(params) => {
                write!(
                    f,
                    "TYPE = CSV COMPRESSION = {:?} \
                     FIELD_DELIMITER = '{}' RECORD_DELIMITER = '{}' QUOTE = '{}' ESCAPE = '{}' \
                     SKIP_HEADER= {} OUTPUT_HEADER= {} \
                     NULL_DISPLAY = '{}' NAN_DISPLAY = '{}'  EMPTY_FIELD_AS = {} BINARY_FORMAT = {} \
                     ERROR_ON_COLUMN_COUNT_MISMATCH = {}",
                    params.compression,
                    escape_string(&params.field_delimiter),
                    escape_string(&params.record_delimiter),
                    escape_string(&params.quote),
                    escape_string(&params.escape),
                    params.headers,
                    params.output_header,
                    escape_string(&params.null_display),
                    escape_string(&params.nan_display),
                    params.empty_field_as,
                    params.binary_format,
                    params.error_on_column_count_mismatch,
                )
            }
            FileFormatParams::Tsv(params) => {
                write!(
                    f,
                    "TYPE = TSV COMPRESSION = {:?} \
                     FIELD_DELIMITER = '{}' RECORD_DELIMITER = '{}' ESCAPE = '{}'  QUOTE = '{}' \
                     SKIP_HEADER = {} \
                     NAN_DISPLAY = '{}'",
                    params.compression,
                    escape_string(&params.field_delimiter),
                    escape_string(&params.record_delimiter),
                    escape_string(&params.escape),
                    escape_string(&params.quote),
                    params.headers,
                    escape_string(&params.nan_display),
                )
            }
            FileFormatParams::Xml(params) => {
                write!(
                    f,
                    "TYPE = XML COMPRESSION = {:?} ROW_TAG = '{}'",
                    params.compression, params.row_tag
                )
            }
            FileFormatParams::Json(params) => {
                write!(f, "TYPE = JSON COMPRESSION = {:?}", params.compression)
            }
            FileFormatParams::NdJson(params) => {
                write!(
                    f,
                    "TYPE = NDJSON, COMPRESSION = {:?} MISSING_FIELD_AS = {} NULL_FIELDS_AA = {}",
                    params.compression, params.missing_field_as, params.null_field_as
                )
            }
            FileFormatParams::Parquet(params) => {
                write!(
                    f,
                    "TYPE = PARQUET MISSING_FIELD_AS = {}",
                    params.missing_field_as
                )
            }
        }
    }
}

pub fn check_row_tag(option: &str) -> std::result::Result<(), String> {
    let len = option.as_bytes().len();
    let (max, min) = (1024, 1);
    if len > max || len < min {
        Err("Expecting a non-empty string containing at most 1024 characters.".to_string())
    } else {
        Ok(())
    }
}

pub fn check_field_delimiter(option: &str) -> std::result::Result<(), String> {
    if option.len() == 1 && (!option.as_bytes()[0].is_ascii_alphanumeric()) {
        Ok(())
    } else {
        Err("Expecting a single one-byte, non-alphanumeric character.".into())
    }
}

/// `\r\n` or u8
pub fn check_record_delimiter(option: &str) -> std::result::Result<(), String> {
    if (option.len() == 1 && (!option.as_bytes()[0].is_ascii_alphanumeric())) || option == "\r\n" {
        Ok(())
    } else {
        Err("Expecting a single one-byte, non-alphanumeric character or '\\r\\n'.".into())
    }
}

fn check_nan_display(nan_display: &str) -> std::result::Result<(), String> {
    check_choices(nan_display, &["nan", "NaN", "null", "NULL"])
}

pub fn check_quote(option: &str) -> std::result::Result<(), String> {
    check_choices(option, &["\'", "\"", "`"])
}

pub fn check_escape(option: &str) -> std::result::Result<(), String> {
    check_choices(option, &["\\", ""])
}

pub fn check_choices(v: &str, choices: &[&str]) -> std::result::Result<(), String> {
    if !choices.contains(&v) {
        let choices = choices
            .iter()
            .map(|s| format!("'{}'", escape_string(s)))
            .collect::<Vec<_>>();
        let choices = choices.join(", ");
        return Err(format!("The valid values are {choices}."));
    }
    Ok(())
}
