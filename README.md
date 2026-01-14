# Bestdori 歌曲谱面 txt 下载器

该脚本会根据 Bestdori 的歌曲元数据，下载指定歌曲 ID 在 CN 区的所有难度（easy/normal/hard/expert/special）的谱面 txt 文件，并保存到本项目的 `output/{songId}/{difficulty}.txt`。

## 运行环境
- Windows（已测试）
- Python 3.8+
- 需可访问 `bestdori.com`

> 你已创建了 `uv venv`，也可以直接用系统 Python 运行，无额外依赖。

## 使用方法

在项目根目录执行：

```powershell
# 进入工作区根目录
cd D:\code\Python\DownloadMusic

# 运行脚本（系统 Python）
python .\scr\download_bestdori_scores.py

# 或者使用 uv 虚拟环境（如果已激活）
uv run python .\scr\download_bestdori_scores.py
```

### 网络问题排查
脚本依赖 Bestdori 的 JSON 元数据接口：
- `https://bestdori.com/api/songs/all.7.json`

若接口被拦截/超时：
- 稍后重试，或更换可访问的网络环境
- 适当调大 `--probe-timeout` / `--download-timeout`，或增加 `--retries`

## 修改歌曲列表
- 打开 `scr/download_bestdori_scores.py`
- 编辑 `SONG_IDS` 列表，添加或删除需要的歌曲 ID

## 说明与注意
- 脚本通过 `https://bestdori.com/api/songs/all.7.json` 解析每首歌的资源目录与文件名，再拼接 txt 下载地址。
- 若 API 被防护拦截或不可访问，脚本会提示错误。此时可稍后重试或在本机浏览器可访问的网络环境运行。
- 下载成功的文件位于 `output/{songId}/{difficulty}.txt`。
