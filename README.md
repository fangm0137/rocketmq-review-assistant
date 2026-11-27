# RocketMQ 代码协作 Review 助手

Apache RocketMQ 开源实践课 · 进阶题目 3

输入一份包含 Java 代码片段的 UTF-8 `.java` 文件。Supervisor 并行发出风格、逻辑和安全三项审查任务。三项结果经同一会话的 LiteTopic 回传后，SummaryAgent 生成 Markdown Review 报告。

## 运行

依赖 JDK 17 或以上、Maven 3.9、Python 3.10 或以上、Apache RocketMQ 5.5.1 二进制发行包。实测用 JDK 23.0.1、Maven 3.9.9 和 Windows 本地 RocketMQ 集群。

先从 Apache 上游安装本项目固定使用的 rocketmq-a2a SDK。

```bash
git clone https://github.com/apache/rocketmq-ai.git
git -C rocketmq-ai checkout 4550db7ea564b9c6dd45bf905d33dc92ffe2da08
mvn -f rocketmq-ai/rocketmq-a2a/pom.xml -Dmaven.test.skip=true -Dcheckstyle.skip=true -Drat.skip=true install
```

在此项目目录构建，`--runtime` 填入实际 RocketMQ 5.5.1 目录，该目录下应有 `lib`。

```bash
mvn -B package dependency:copy-dependencies
python run_lab.py --runtime /path/to/rocketmq-5.5.1
```

在 Windows PowerShell 中可以传入实际 JDK 路径。

```powershell
python .\run_lab.py --runtime 'C:\path\to\rocketmq-5.5.1' --java 'C:\path\to\jdk\bin\java.exe'
```

默认输入为 [ReviewSample.java](examples/ReviewSample.java)。用 `--source path/to/Code.java` 指定其他 Java 源码。解析器读取语法树，不运行输入代码。规则识别变量命名、边界条件、动态 SQL 和 HTML 输出等线索，结果附行号和修复建议，需人工核对代码上下文。

脚本启动独立的 NameServer 和 Broker + Proxy，为该批次创建隔离的数据目录。每次只运行一个 Supervisor 会话。输出位置由终端的 `EVIDENCE_DIR` 标明。主要产物为 `review.md`、`reviews.json`、`trace.json`、应用收发日志、Broker 查询记录和 RocketMQ 原生轨迹查询记录。

## 在 RocketMQ Dashboard 验收

传入 Apache RocketMQ Dashboard 2.1.0 的 JAR。

```bash
python run_lab.py --runtime /path/to/rocketmq-5.5.1 --dashboard-jar /path/to/rocketmq-dashboard.jar
```

看到 `DASHBOARD_READY` 后打开 `http://127.0.0.1:18082`。进入「消息轨迹」，选中 `RMQ_SYS_TRACE_TOPIC` 作为轨迹存储主题。在 Message Key 页签选择业务 Topic，填入 `trace.json` 中的 `key` 并查询，点击消息行的轨迹详情。也可以用 `tools/capture_dashboard.cjs` 批量保存全部轨迹，需要本机安装 Playwright 和 Chromium 或 Edge。

```bash
node tools/capture_dashboard.cjs <EVIDENCE_DIR> <browser-executable>
```

更多说明见 [系统架构与验收](docs/architecture.md)。
