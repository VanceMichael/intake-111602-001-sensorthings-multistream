# 环境站资料查看器

用于检查授权来源的一份原始资料并留存接收凭据。当前提供资料预览、原件接收、凭据列表与摘要回读，资料源可以在本地独立运行。

## 环境与运行

使用 Node.js 22.23.4，无第三方运行包。安装、检查与测试：

```sh
npm ci --ignore-scripts --offline
npm run build
npm test
npm run demo
```

`demo` 临时启动来源和查看服务，展示当前资料并核对保存后的原件；结束时关闭所有监听并移除临时目录。不会连接外部来源。`test` 校验已有预览、资料控制和原件持久化；测试通过只说明当前版本已有能力正常。

需要交互时分别运行 `npm run source` 与 `npm start`。来源默认监听本机 4310 端口，应用默认监听 4320；可用 `PORT`、`SOURCE_URL`、`DATA_DIR` 配置。`GET /preview` 获取预览，`POST /receipts` 接收当前资料，`GET /receipts` 查询凭据，`GET /objects/<摘要>` 回读原件。应用只允许配置到本机的资料服务。

### 按观测流采集

- `POST /intake`，请求体 `{"kind":"Datastream"|"MultiDatastream","streamId":7}`，为观测流建立采集任务并立即领取（可另传 `observationsPath` 覆盖默认入口）；
- `POST /intake/<datastream-7|multi-8>/run` 继续领取：翻页收集新观测、追加更正版本、补齐缺失的定义文档与流快照；
- `GET /intake` 列出任务摘要，`GET /intake/<slug>` 查看任务与可用/待补观测，`GET /intake/<slug>/observations?status=ready|pending` 只看其中一类。

可用观测只包含解释依据齐全的最新版本，每个读数都标明 ObservedProperty、单位、定义版本引用与传感器；依据暂缺的观测留在 `pending` 并说明原因，原文仍按摘要留存。流没有观测（`empty`）、站点明确空值（`valueKind:"null"`）与暂时缺失（`pending`）是三种不同状态。

## 目录

`source/` 负责资料源的发布日期和传输状态；`src/decode.mjs` 解释一份来源资料；`src/sensorthings.mjs` 按字段身份解析普通流分页、dataArray 与流元数据；`src/intake.mjs` 实现按流领取、版本绑定、解释补齐与可用/待补视图；`src/catalog.mjs` 管理不可变原件、接收凭据及采集任务状态；`src/app.mjs` 提供查看服务。资料含义及来源控制见 `docs/资料接口.md`。

## 容器

```sh
docker build -t sensorthings-multistream .
docker run --rm --network none sensorthings-multistream
```

镜像固定 Node.js 版本，构建期间不下载 npm 运行依赖。运行产生的原件保存在 `DATA_DIR`，不应写回来源文件。
