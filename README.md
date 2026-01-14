# Bestdori 歌曲谱面 txt 下载器

该脚本会根据 Bestdori 的歌曲元数据，下载指定歌曲 ID 在 CN 区的所有难度（easy/normal/hard/expert/special）的谱面 txt 文件，并保存到本项目的 `output/musiccore/{曲名文件夹}/{difficulty}.txt`。

> 输出目录默认用日文歌名作为文件夹名；会对 Windows 非法字符做替换，并写入一个 `.bestdori_song_id` 标记文件用于避免同名冲突。

## 运行环境

- Windows（已测试）
- Python 3.8+
- 需可访问 `bestdori.com`

> 你已创建了 `uv venv`，也可以直接用系统 Python 运行，无额外依赖。

## 使用方法

在项目根目录执行：

```powershell
# 进入工作区根目录
cd DownloadMusic

# 运行脚本（系统 Python）
python .\scr\download_bestdori_scores.py

# 或者使用 uv 虚拟环境（如果已激活）
uv run python .\scr\download_bestdori_scores.py
```

### 指定歌曲 ID（推荐）

脚本支持在命令行直接传入歌曲 ID：空格分隔或逗号分隔都可以。

```powershell
# 下载指定歌曲（空格分隔）
python .\scr\download_bestdori_scores.py 186 189 344

# 下载指定歌曲（逗号分隔）
python .\scr\download_bestdori_scores.py 186,189,344

# 混用也可以（内部会统一解析）
python .\scr\download_bestdori_scores.py 186,189 344
```

### 默认下载范围

若不传入任何 ID 参数，将使用脚本内置列表：

- 1~750
- 10001~10010

（内置列表去重并排序后执行）

### 参数说明

```text
python .\scr\download_bestdori_scores.py [ids ...] [--print-urls] [--no-probe] [--dry-run]
									 [--probe-timeout N] [--download-timeout N] [--retries N]
```

- `ids`：歌曲 ID 列表（可选）。支持 `489 545` 或 `489,545`。
- `--print-urls`：打印解析出来的各难度下载链接（不一定存在）。
- `--no-probe`：配合 `--print-urls` 使用；不做网络探测，直接输出构造的链接（可能包含不存在的资源）。
- `--dry-run`：仅演练；不下载/不写文件/不更新 JSON，可与 `--print-urls` 组合使用。
- `--probe-timeout`：探测资源是否存在的超时时间（秒，Range 请求），默认 6。
- `--download-timeout`：下载谱面文件的超时时间（秒），默认 25。
- `--retries`：网络请求重试次数，默认 2。

### 常用组合示例

```powershell
# 只看链接（会 probe 判断是否存在）
python .\scr\download_bestdori_scores.py 604 --print-urls

# 只构造链接，不 probe（速度快，但可能有不存在的资源）
python .\scr\download_bestdori_scores.py 604 --print-urls --no-probe

# 网络不稳时：延长超时 + 增加重试
python .\scr\download_bestdori_scores.py 604 --probe-timeout 12 --download-timeout 60 --retries 5
```

### 网络问题排查

脚本依赖 Bestdori 的 JSON 元数据接口：

- `https://bestdori.com/api/songs/all.7.json`

若接口被拦截/超时：

- 稍后重试，或更换可访问的网络环境
- 适当调大 `--probe-timeout` / `--download-timeout`，或增加 `--retries`

## 修改歌曲列表

一般不需要改代码：直接在命令行传入 `ids` 即可。

如果你确实想修改默认下载范围：

- 打开 `scr/download_bestdori_scores.py`
- 编辑 `SONG_IDS` 列表

建议优先用命令行参数，避免多人/多机环境下的默认行为不一致。

## 失败列表与重试

脚本会在 `output/musiccore/` 下生成失败记录：`_failures_cn_YYYYMMDD_HHMMSS.json`。

可以用辅助脚本把所有失败歌曲 ID 汇总出来：

```powershell
# 从默认 glob（output/musiccore/_failures_cn_*.json）读取并输出到控制台
python .\scr\id_list.py

# 只取最新的一份 failures 文件
python .\scr\id_list.py --latest

# 指定某一份 failures 文件
python .\scr\id_list.py --file output/musiccore/_failures_cn_20260115_030002.json

# 将汇总后的 id 写入文本文件（每行一个）
python .\scr\id_list.py --glob output/musiccore/_failures_cn_*.json --out failures_ids.txt
```

## 说明与注意

- 脚本通过 `https://bestdori.com/api/songs/all.7.json` 解析每首歌的资源目录与文件名，再拼接 txt 下载地址。
- 若 API 被防护拦截或不可访问，脚本会提示错误。此时可稍后重试或在本机浏览器可访问的网络环境运行。
- 下载成功的文件位于 `output/musiccore/{曲名文件夹}/{difficulty}.txt`。
- 部分歌曲下载失败请重试或查看代码内注释，部分歌曲及难度仅在jp实装
