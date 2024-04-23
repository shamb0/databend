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

use std::path::Path;

use chrono::DateTime;
use chrono::Utc;
use databend_common_base::runtime::execute_futures_in_parallel;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::principal::UserIdentity;
use futures::TryStreamExt;
use opendal::EntryMode;
use opendal::Metadata;
use opendal::Metakey;
use opendal::Operator;
use regex::Regex;

use crate::init_operator;
use crate::DataOperator;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub enum StageFileStatus {
    NeedCopy,
    AlreadyCopied,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct StageFileInfo {
    pub path: String,
    pub size: u64,
    pub md5: Option<String>,
    pub last_modified: DateTime<Utc>,
    pub etag: Option<String>,
    pub status: StageFileStatus,
    pub creator: Option<UserIdentity>,
}

impl StageFileInfo {
    pub fn new(path: String, meta: &Metadata) -> StageFileInfo {
        StageFileInfo {
            path,
            size: meta.content_length(),
            md5: meta.content_md5().map(str::to_string),
            last_modified: meta.last_modified().unwrap_or_default(),
            etag: meta.etag().map(str::to_string),
            status: StageFileStatus::NeedCopy,
            creator: None,
        }
    }

    /// NOTE: update this query when add new meta
    pub fn meta_query() -> flagset::FlagSet<Metakey> {
        Metakey::ContentLength | Metakey::ContentMd5 | Metakey::LastModified | Metakey::Etag
    }
}

pub fn init_stage_operator(stage_info: &StageInfo) -> Result<Operator> {
    if stage_info.stage_type == StageType::External {
        Ok(init_operator(&stage_info.stage_params.storage)?)
    } else {
        let stage_prefix = stage_info.stage_prefix();
        let param = DataOperator::instance()
            .params()
            .map_root(|path| format!("{path}/{stage_prefix}"));

        Ok(init_operator(&param)?)
    }
}
/// select * from @s1/<path> (FILES => <files> PATTERN => <pattern>)
/// copy from @s1/<path> FILES = <files> PATTERN => <pattern>
#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq, Debug)]
pub struct StageFilesInfo {
    pub path: String,
    pub files: Option<Vec<String>>,
    pub pattern: Option<String>,
}

impl StageFilesInfo {
    fn get_pattern(&self) -> Result<Option<Regex>> {
        match &self.pattern {
            Some(pattern) => match Regex::new(&format!("^{pattern}$")) {
                Ok(r) => Ok(Some(r)),
                Err(e) => Err(ErrorCode::SyntaxException(format!(
                    "Pattern format invalid, got:{}, error:{:?}",
                    pattern, e
                ))),
            },
            None => Ok(None),
        }
    }

    #[async_backtrace::framed]
    pub async fn list(
        &self,
        operator: &Operator,
        thread_num: usize,
        max_files: Option<usize>,
    ) -> Result<Vec<StageFileInfo>> {
        if self.path == STDIN_FD {
            return Ok(vec![stdin_stage_info()?]);
        }

        let max_files = max_files.unwrap_or(usize::MAX);
        if let Some(files) = &self.files {
            let file_infos = self
                .stat_concurrent(operator, thread_num, max_files, files)
                .await?;
            let mut res = Vec::with_capacity(file_infos.len());

            for file_info in file_infos {
                match file_info {
                    Ok((path, meta)) if meta.is_dir() => {
                        return Err(ErrorCode::BadArguments(format!("{path} is not a file")));
                    }
                    Ok((path, meta)) => res.push(StageFileInfo::new(path, &meta)),
                    Err(e) => {
                        return Err(e);
                    }
                }
            }
            Ok(res)
        } else {
            let pattern = self.get_pattern()?;
            StageFilesInfo::list_files_with_pattern(operator, &self.path, pattern, max_files).await
        }
    }

    #[async_backtrace::framed]
    pub async fn first_file(&self, operator: &Operator) -> Result<StageFileInfo> {
        // We only fetch first file.
        let mut files = self.list(operator, 1, Some(1)).await?;
        files
            .pop()
            .ok_or_else(|| ErrorCode::BadArguments("no file found"))
    }

    pub fn blocking_first_file(&self, operator: &Operator) -> Result<StageFileInfo> {
        let mut files = self.blocking_list(operator, Some(1))?;
        files
            .pop()
            .ok_or_else(|| ErrorCode::BadArguments("no file found"))
    }

    pub fn blocking_list(
        &self,
        operator: &Operator,
        max_files: Option<usize>,
    ) -> Result<Vec<StageFileInfo>> {
        let max_files = max_files.unwrap_or(usize::MAX);
        if let Some(files) = &self.files {
            let mut res = Vec::new();
            for file in files {
                let full_path = Path::new(&self.path)
                    .join(file)
                    .to_string_lossy()
                    .trim_start_matches('/')
                    .to_string();
                let meta = operator.blocking().stat(&full_path)?;
                if meta.mode().is_file() {
                    res.push(StageFileInfo::new(full_path, &meta))
                } else {
                    return Err(ErrorCode::BadArguments(format!(
                        "{full_path} is not a file"
                    )));
                }
                if res.len() == max_files {
                    return Ok(res);
                }
            }
            Ok(res)
        } else {
            let pattern = self.get_pattern()?;
            blocking_list_files_with_pattern(operator, &self.path, pattern, max_files)
        }
    }

    #[async_backtrace::framed]
    pub async fn list_files_with_pattern(
        operator: &Operator,
        path: &str,
        pattern: Option<Regex>,
        max_files: usize,
    ) -> Result<Vec<StageFileInfo>> {
        if path == STDIN_FD {
            return Ok(vec![stdin_stage_info()?]);
        }
        let mut files = Vec::new();
        let prefix_len = if path == "/" { 0 } else { path.len() };
        let prefix_meta = operator.stat(path).await;
        match prefix_meta {
            Ok(meta) if meta.is_file() => {
                files.push(StageFileInfo::new(path.to_string(), &meta));
            }
            Err(e) if e.kind() != opendal::ErrorKind::NotFound => {
                return Err(e.into());
            }
            _ => {}
        };
        let mut lister = operator
            .lister_with(path)
            .recursive(true)
            .metakey(StageFileInfo::meta_query())
            .await?;

        if files.len() == max_files {
            return Ok(files);
        }
        while let Some(obj) = lister.try_next().await? {
            let meta = obj.metadata();
            if check_file(&obj.path()[prefix_len..], meta.mode(), &pattern) {
                files.push(StageFileInfo::new(obj.path().to_string(), meta));
                if files.len() == max_files {
                    return Ok(files);
                }
            }
        }
        Ok(files)
    }

    /// Stat files concurrently.
    #[async_backtrace::framed]
    pub async fn stat_concurrent(
        &self,
        operator: &Operator,
        thread_num: usize,
        max_files: usize,
        files: &[String],
    ) -> Result<Vec<Result<(String, Metadata)>>> {
        if max_files == 1 {
            let Some(file) = files.first() else {
                return Ok(vec![]);
            };
            let full_path = Path::new(&self.path)
                .join(file)
                .to_string_lossy()
                .trim_start_matches('/')
                .to_string();
            let meta = operator.stat(&full_path).await;
            return Ok(vec![meta.map(|m| (full_path, m)).map_err(Into::into)]);
        }

        // This clone is required to make sure we are not referring `file: &String` in the closure
        let tasks = files.iter().take(max_files).cloned().map(|file| {
            let full_path = Path::new(&self.path)
                .join(file)
                .to_string_lossy()
                .trim_start_matches('/')
                .to_string();
            let operator = operator.clone();
            async move {
                let meta = operator.stat(&full_path).await?;
                Ok((full_path, meta))
            }
        });

        execute_futures_in_parallel(
            tasks,
            thread_num * 5,
            thread_num * 10,
            "batch-stat-file-worker".to_owned(),
        )
        .await
    }
}

fn check_file(path: &str, mode: EntryMode, pattern: &Option<Regex>) -> bool {
    if !path.is_empty() && mode.is_file() {
        pattern.as_ref().map_or(true, |p| p.is_match(path))
    } else {
        false
    }
}

fn blocking_list_files_with_pattern(
    operator: &Operator,
    path: &str,
    pattern: Option<Regex>,
    max_files: usize,
) -> Result<Vec<StageFileInfo>> {
    if path == STDIN_FD {
        return Ok(vec![stdin_stage_info()?]);
    }
    let operator = operator.blocking();
    let mut files = Vec::new();
    let prefix_meta = operator.stat(path);
    match prefix_meta {
        Ok(meta) if meta.is_file() => {
            files.push(StageFileInfo::new(path.to_string(), &meta));
        }
        Err(e) if e.kind() != opendal::ErrorKind::NotFound => {
            return Err(e.into());
        }
        _ => {}
    };
    let prefix_len = if path == "/" { 0 } else { path.len() };
    let list = operator
        .lister_with(path)
        .recursive(true)
        .metakey(StageFileInfo::meta_query())
        .call()?;
    if files.len() == max_files {
        return Ok(files);
    }
    for obj in list {
        let obj = obj?;
        let meta = obj.metadata();
        if check_file(&obj.path()[prefix_len..], meta.mode(), &pattern) {
            files.push(StageFileInfo::new(obj.path().to_string(), meta));
            if files.len() == max_files {
                return Ok(files);
            }
        }
    }
    Ok(files)
}

pub const STDIN_FD: &str = "/dev/fd/0";

fn stdin_stage_info() -> Result<StageFileInfo> {
    Ok(StageFileInfo {
        path: STDIN_FD.to_string(),
        size: u64::MAX,
        md5: None,
        last_modified: Utc::now(),
        etag: None,
        status: StageFileStatus::NeedCopy,
        creator: None,
    })
}
