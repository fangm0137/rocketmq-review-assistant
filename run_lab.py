"""Run an isolated Apache RocketMQ 5.5.1 Broker + Proxy and the review demo."""

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import time

ROOT = Path(__file__).resolve().parent
STUDENT = "2601210305"
TASK = f"rmqai-{STUDENT}-review-task"
RESPONSE = f"rmqai-{STUDENT}-review-response"
NAMESRV = "127.0.0.1:19876"
PROXY = "127.0.0.1:18081"
BROKER = "127.0.0.1:11911"
ROLES = ("STYLE", "LOGIC", "SECURITY", "SUMMARY")


def group(role):
    return f"GID_{STUDENT}_{role.lower()}"


def connected(port):
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=.5):
            return True
    except OSError:
        return False


def wait_ready(process, port, log):
    for _ in range(120):
        if process.poll() is not None:
            raise RuntimeError(f"RocketMQ stopped during startup. See {log}")
        if connected(port):
            return
        time.sleep(.5)
    raise TimeoutError(f"RocketMQ port {port} was not ready. See {log}")


def run(args, output, env, timeout=120):
    with output.open("wb") as log:
        subprocess.run(args, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                       check=True, timeout=timeout)
    return output.read_text(encoding="utf-8", errors="replace")


def start(args, output, env):
    with output.open("wb") as log:
        return subprocess.Popen(args, cwd=ROOT, env=env, stdout=log, stderr=subprocess.STDOUT,
                                creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))


def config_files(run_dir):
    namesrv = run_dir / "namesrv.properties"
    namesrv.write_text("bindAddress=127.0.0.1\nlistenPort=19876\n", encoding="ascii")
    broker = run_dir / "broker.properties"
    # java.util.Properties requires non-ASCII characters to use Unicode escapes.
    store = (run_dir / "store").as_posix().encode("unicode_escape").decode("ascii")
    broker.write_text(
        "brokerClusterName=ReviewCourse\nbrokerName=review-broker\nbrokerId=0\n"
        "brokerRole=ASYNC_MASTER\nflushDiskType=ASYNC_FLUSH\n"
        f"namesrvAddr={NAMESRV}\nbrokerIP1=127.0.0.1\nbrokerIP2=127.0.0.1\n"
        "bindAddress=127.0.0.1\nlistenPort=11911\n"
        f"storePathRootDir={store}\nstorePathCommitLog={store}/commitlog\n"
        "mappedFileSizeCommitLog=67108864\nmaxHashSlotNum=50000\nmaxIndexNum=200000\n"
        "autoCreateTopicEnable=false\nautoCreateSubscriptionGroup=false\n"
        "traceTopicEnable=true\nenableLmq=true\nenableMultiDispatch=true\n"
        "sendMessageThreadPoolNums=4\npullMessageThreadPoolNums=4\n",
        encoding="ascii",
    )
    proxy = run_dir / "proxy.json"
    proxy.write_text(json.dumps({
        "localServeAddr": "127.0.0.1", "remotingListenPort": 18080,
        "grpcServerPort": 18081, "namesrvAddr": NAMESRV,
        "rocketMQClusterName": "ReviewCourse", "traceOn": True,
        "grpcProducerThreadPoolNums": 4, "grpcConsumerThreadPoolNums": 4,
        "grpcClientManagerThreadPoolNums": 2, "grpcRouteThreadPoolNums": 2,
    }, indent=2), encoding="utf-8")
    return namesrv, broker, proxy


def verify(run_dir, admin, java, env, resume=False):
    def query(filename, command, *opts):
        evidence = run_dir / filename
        if resume and evidence.is_file() and evidence.stat().st_size:
            return evidence.read_text(encoding="utf-8", errors="replace")
        return admin(filename, command, *opts)

    events = []
    for line in (run_dir / "review-run.log").read_text(encoding="utf-8", errors="replace").splitlines():
        if not line.startswith("TRACE\t"):
            continue
        _, timestamp, session, event, role, topic, consumer_group, key, msg_id = line.split("\t")
        events.append(dict(time=timestamp, session=session, event=event, role=role, topic=topic,
                           group=consumer_group, key=key, message_id=msg_id))
    session_ids = {event["session"] for event in events}
    if len(session_ids) != 1:
        raise RuntimeError("A trace recording must contain exactly one session")
    session, = session_ids
    if not session.startswith(STUDENT + "-"):
        raise RuntimeError("Session is missing the student prefix")
    rows = []
    for role in ROLES:
        for kind, topic, stage_name in (("TASK", TASK, "Normal"), ("LITE", RESPONSE, "Lite")):
            selected = [e for e in events if e["role"] == role and e["topic"] == topic]
            sent = next(e for e in selected if e["event"] == kind + "_SEND")
            received = next(e for e in selected if e["event"] == kind + "_RECEIVE")
            if sent["message_id"] != received["message_id"] or sent["key"] != received["key"]:
                raise RuntimeError(f"Message correlation failed for {role}/{kind}")
            filename = f"message-{role.lower()}-{kind.lower()}.log"
            text = query(filename, "queryMsgByKey", "-t", topic, "-k", sent["key"])
            broker_confirmed = sent["message_id"].casefold() in re.sub(r"[^0-9a-zA-Z]", "", text).casefold()
            # Some Broker builds print the physical offset ID rather than the client ID in this listing.
            indexed = bool(re.search(r"(?m)^[0-9A-Fa-f]{24,}\s+\d+", text))
            if not (broker_confirmed or indexed):
                raise RuntimeError(f"Broker query returned no indexed message for {sent['key']}")
            rows.append(dict(role=role, type=stage_name, topic=topic, group=received["group"],
                             key=sent["key"], message_id=sent["message_id"], sent=sent["time"],
                             received=received["time"], broker_query=filename))
    lite = query("lite-topic.log", "getLiteTopicInfo", "-p", RESPONSE, "-l", session)
    offsets = query("lite-group.log", "getLiteGroupInfo", "-p", RESPONSE,
                    "-g", group("supervisor"), "-l", session)
    if session not in lite or not re.search(r"(?m)^review-broker\s+4\s+4\s+0\s", offsets):
        raise RuntimeError("Broker did not confirm the session and all four Lite acknowledgements")
    verified_trace = run_dir / "trace.json"
    if not (resume and verified_trace.is_file()):
        run(java + [str(ROOT / "tools" / "PublishTrace.java"), NAMESRV,
                    str(run_dir / "review-run.log"), group("supervisor")],
            run_dir / "trace-export.log", env, 90)
    for row in rows:
        trace_file = f"native-trace-{row['role'].lower()}-{row['type'].lower()}.log"
        for _ in range(6):
            trace = (query if verified_trace.is_file() else admin)(
                trace_file, "queryMsgTraceById", "-i", row["message_id"])
            if re.search(r"(?m)^Pub\s+", trace) and re.search(r"(?m)^Sub\s+" + row["group"], trace):
                break
            time.sleep(1)
        else:
            raise RuntimeError(f"Native trace did not return both stages for {row['message_id']}")
        row["native_trace"] = trace_file
    verified_trace.write_text(json.dumps({
        "session": session, "expected": 8, "matched": len(rows), "message_trace": rows
    }, indent=2), encoding="utf-8")
    print(f"TRACE_PASS session={session} normal=4 lite=4 matched=8", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--runtime", required=True, type=Path, help="RocketMQ 5.5.1 distribution directory")
    parser.add_argument("--java", default="java", help="Java executable")
    parser.add_argument("--source", default=ROOT / "examples/ReviewSample.java", type=Path)
    parser.add_argument("--resume", type=Path, help="Reopen a completed run and verify its preserved Broker store")
    parser.add_argument("--dashboard-jar", type=Path, help="Optional Apache RocketMQ Dashboard JAR")
    parser.add_argument("--dashboard-seconds", type=int, default=240)
    args = parser.parse_args()
    runtime = args.runtime.resolve()
    libraries = runtime / "lib"
    app = ROOT / "target"
    if not (app / "classes/course/review/SupervisorAgent.class").is_file() or not (app / "dependency").is_dir():
        parser.error("Build the Maven project and run dependency:copy-dependencies first.")
    if not (libraries / "rocketmq-proxy-5.5.1.jar").is_file():
        parser.error("--runtime must point to a RocketMQ 5.5.1 distribution with Proxy.")
    for port in (19876, 11911, 11909, 11912, 18080, 18081):
        if connected(port):
            parser.error(f"TCP port {port} is already in use.")
    if args.dashboard_jar and (not args.dashboard_jar.is_file() or connected(18082)):
        parser.error("Dashboard JAR is missing or port 18082 is already in use.")
    if args.resume:
        run_dir = args.resume.resolve()
        if run_dir.parent != (ROOT / "runs").resolve() or not (run_dir / "store").is_dir():
            parser.error("--resume must name an existing evidence directory in runs/")
        if "REVIEW_PASS " not in (run_dir / "review-run.log").read_text(encoding="utf-8", errors="replace"):
            parser.error("--resume requires previously completed application execution")
    else:
        run_dir = ROOT / "runs" / datetime.now().strftime("%Y%m%d-%H%M%S-%f")
        run_dir.mkdir(parents=True)
    print(f"EVIDENCE_DIR={run_dir}", flush=True)
    namesrv_config, broker_config, proxy_config = config_files(run_dir)
    env = {**os.environ, "ROCKETMQ_HOME": str(runtime), "NAMESRV_ADDR": NAMESRV}
    java = [args.java, "-Dfile.encoding=UTF-8", f"-Duser.home={run_dir}",
            f"-Drocketmq.home.dir={runtime}", "-cp", str(libraries / "*")]

    def admin(logfile, command, *opts):
        return run(java + ["org.apache.rocketmq.tools.command.MQAdminStartup", command, "-n", NAMESRV, *opts],
                   run_dir / logfile, env, 35)

    services = []
    try:
        log = run_dir / ("nameserver-resume.log" if args.resume else "nameserver.log")
        namesrv = start(java + ["org.apache.rocketmq.namesrv.NamesrvStartup", "-c", str(namesrv_config)],
                        log, env)
        services.append(namesrv)
        wait_ready(namesrv, 19876, log)
        log = run_dir / ("broker-proxy-resume.log" if args.resume else "broker-proxy.log")
        broker = start(java[:1] + ["-Xms256m", "-Xmx768m", "-XX:MaxDirectMemorySize=256m",
                                   "--add-opens=java.base/java.nio=ALL-UNNAMED",
                                   "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"] + java[1:] +
                       ["org.apache.rocketmq.proxy.ProxyStartup", "-pm", "local", "-n", NAMESRV,
                        "-bc", str(broker_config), "-pc", str(proxy_config)], log, env)
        services.append(broker)
        wait_ready(broker, 18081, log)
        for _ in range(15):
            if "review-broker" in admin("cluster-resume.log" if args.resume else "cluster.log", "clusterList"):
                break
            time.sleep(1)
        else:
            raise RuntimeError("NameServer did not register review-broker")
        if args.resume:
            output = (run_dir / "review-run.log").read_text(encoding="utf-8", errors="replace")
        else:
            admin("normal-topic.log", "updateTopic", "-b", BROKER, "-t", TASK, "-r", "4", "-w", "4")
            admin("lite-parent.log", "updateTopic", "-b", BROKER, "-t", RESPONSE, "-r", "4", "-w", "4",
                  "-a", "+message.type=LITE")
            for role in ROLES:
                admin(f"group-{role.lower()}.log", "updateSubGroup", "-b", BROKER, "-g", group(role))
            admin("group-supervisor.log", "updateSubGroup", "-b", BROKER, "-g", group("supervisor"),
                  "--attributes", f"+lite.bind.topic={RESPONSE},+lite.sub.model=Exclusive")
            java_client = [args.java, "-Dfile.encoding=UTF-8", f"-Duser.home={run_dir}", "-cp",
                           os.pathsep.join((str(app / "classes"), str(app / "dependency" / "*")))]
            output = run(java_client + ["course.review.SupervisorAgent", PROXY,
                                       str(args.source.resolve()), str(run_dir)],
                         run_dir / "review-run.log", env, 240)
        print(next(line for line in output.splitlines() if line.startswith("REVIEW_PASS ")), flush=True)
        verify(run_dir, admin, java, env, resume=bool(args.resume))
        if args.dashboard_jar:
            log = run_dir / "dashboard.log"
            dashboard = start([args.java, "-Xmx384m", f"-Duser.home={run_dir}", "-jar",
                               str(args.dashboard_jar.resolve()), "--server.address=127.0.0.1",
                               "--server.port=18082", f"--rocketmq.config.namesrvAddrs[0]={NAMESRV}",
                               f"--rocketmq.config.dataPath={run_dir / 'dashboard-data'}"],
                              log, env)
            services.append(dashboard)
            wait_ready(dashboard, 18082, log)
            print("DASHBOARD_READY http://127.0.0.1:18082", flush=True)
            until = time.monotonic() + args.dashboard_seconds
            while time.monotonic() < until and not (run_dir / "capture.done").exists():
                time.sleep(1)
    finally:
        for service in reversed(services):
            if service.poll() is None:
                service.terminate()
            service.wait(timeout=15)


if __name__ == "__main__":
    main()
