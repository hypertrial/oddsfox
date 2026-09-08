"""Explicit local commands; server clients use the API to preserve one writer."""

import argparse
import json
import secrets
import sys
from pathlib import Path

from oddsfox.compiler import blank_ir, compile_local, replay
from oddsfox.demo import load_demo
from oddsfox.evaluation import evaluate
from oddsfox.ingest import fetch, import_capture
from oddsfox.ir import SemanticIR, strict_json
from oddsfox.pipeline import Pipeline
from oddsfox.store import Store


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="OddsFox — local evidence-backed contract research (MIT)"
    )
    p.add_argument("--data", type=Path, default=Path(".oddsfox"), help="local dataset directory")
    sub = p.add_subparsers(dest="command", required=True)
    serve = sub.add_parser("serve", help="serve local report and API")
    serve.add_argument("--port", type=int, default=8777)
    serve.add_argument(
        "--model",
        type=Path,
        action="append",
        default=[],
        help="one local quantized explanation model directory",
    )
    serve.add_argument("--producer-model", type=Path, action="append", default=[])
    serve.add_argument("--evaluator-model", type=Path, action="append", default=[])
    serve.add_argument(
        "--enable-consensus-publication",
        action="store_true",
        help="allow LOCAL_MODEL_CONSENSUS publication from the running server",
    )
    serve.add_argument(
        "--no-sync",
        action="store_true",
        help="disable automatic discovery (for synthetic demos/offline research)",
    )
    sync = sub.add_parser("sync", help="discover all qualifying events from the two public venues")
    sync.add_argument("--venue", action="append", choices=["kalshi", "polymarket"])
    demo = sub.add_parser(
        "demo", help="load clearly marked synthetic examples into an empty dataset"
    )
    demo.add_argument("--approve-synthetic", action="store_true")
    capture = sub.add_parser("capture", help="retrieve bounded native market IDs")
    capture.add_argument("venue", choices=["polymarket", "kalshi"])
    capture.add_argument("ids", nargs="+")
    imp = sub.add_parser("import", help="import an exact captured market JSON file")
    imp.add_argument("venue", choices=["polymarket", "kalshi"])
    imp.add_argument("file", type=Path)
    imp.add_argument("--documents", type=Path, help="JSON list of captured referenced documents")
    draft = sub.add_parser("draft", help="emit an explicitly unknown IR candidate")
    draft.add_argument("contract_id")
    interpret = sub.add_parser("interpret", help="validate and save a candidate IR; never approves")
    interpret.add_argument("file", type=Path)
    interpret.add_argument("--derivations", type=Path)
    registry = sub.add_parser("register", help="review an exact canonical observation definition")
    registry.add_argument("canonical_id")
    registry.add_argument("file", type=Path)
    registry.add_argument("--reviewer", required=True)
    registry.add_argument("--rationale", required=True)
    review = sub.add_parser("review", help="approve or withdraw an exact interpretation")
    review.add_argument("interpretation_id")
    review.add_argument("--reviewer", required=True)
    review.add_argument("--rationale", required=True)
    review.add_argument("--approve", action="store_true", help="otherwise reject/withdraw")
    review.add_argument("--governing-material-complete", action="store_true")
    model = sub.add_parser("compile", help="run one explicitly configured local MLX model")
    model.add_argument("contract_id")
    model.add_argument("model_path", type=Path)
    model.add_argument("--unconstrained", action="store_true")
    model.add_argument("--max-tokens", type=int, default=8192)
    rep = sub.add_parser("replay", help="replay an immutable response without inference")
    rep.add_argument("contract_id")
    rep.add_argument("response_artifact")
    rep.add_argument("config_id")
    sub.add_parser("compare", help="derive provisional comparisons")
    publish = sub.add_parser(
        "publish", help="publish current proven claims with human review or consensus"
    )
    publish.add_argument(
        "--allow-consensus",
        action="store_true",
        help="accept current unanimous producer-panel approvals; human rejection still vetoes",
    )
    sub.add_parser("status", help="show attempts, freshness, failures and reviews")
    export = sub.add_parser("export")
    export.add_argument("--output", type=Path)
    export.add_argument("--format", choices=["json", "parquet"], default="json")
    export.add_argument("--history", action="store_true")
    schema = sub.add_parser("schema", help="emit the public language-independent JSON Schema")
    schema.add_argument("--output", type=Path)
    bench = sub.add_parser("benchmark", help="score frozen labels and original pre-review outputs")
    bench.add_argument("benchmark", type=Path)
    bench.add_argument("run", type=Path)
    bench.add_argument("--output", type=Path, required=True)
    validate = sub.add_parser(
        "validate", help="complete two-venue scans, freeze consensus labels, and score producers"
    )
    validate.add_argument("--output", type=Path, required=True)
    validate.add_argument("--producer-model", type=Path, action="append", default=[], required=True)
    validate.add_argument(
        "--evaluator-model", type=Path, action="append", default=[], required=True
    )
    validate.add_argument("--skip-sync", action="store_true")
    approve = sub.add_parser(
        "consensus-approve",
        help="record a unanimous producer-panel approval; never writes a human review",
    )
    approve.add_argument("interpretation_id")
    approve.add_argument("--producer-model", type=Path, action="append", required=True)
    backup = sub.add_parser("backup", help="checkpoint and copy the stopped application's dataset")
    backup.add_argument("destination", type=Path)
    restore = sub.add_parser("restore", help="verify and restore a backup into a new dataset")
    restore.add_argument("backup", type=Path)
    migrate = sub.add_parser("migrate", help="upgrade a stopped dataset after a verified backup")
    migrate.add_argument("--backup", type=Path, required=True)
    return p


def write_json(value, destination: Path | None = None):
    raw = json.dumps(value, indent=2, ensure_ascii=False, allow_nan=False) + "\n"
    if destination:
        with destination.open("x", encoding="utf-8") as f:
            f.write(raw)
    else:
        print(raw, end="")


def main(argv: list[str] | None = None) -> int:
    args = parser().parse_args(argv)
    try:
        if args.command == "schema":
            write_json(SemanticIR.model_json_schema(), args.output)
            return 0
        if args.command == "benchmark":
            write_json(
                evaluate(
                    strict_json(args.benchmark.read_bytes()), strict_json(args.run.read_bytes())
                ),
                args.output,
            )
            return 0
        if args.command == "restore":
            version = Store.restore(args.backup, args.data)
            write_json(
                {"restored": str(args.data), "version": version, "migration_required": version != 4}
            )
            return 0
        if args.command == "migrate":
            write_json(Store.migrate(args.data, args.backup))
            return 0
        store = Store(args.data)
        pipeline = Pipeline(store, allow_consensus=getattr(args, "allow_consensus", False))
        try:
            match args.command:
                case "serve":
                    import uvicorn

                    from oddsfox.app import create_app
                    from oddsfox.judge import disjoint_panels
                    from oddsfox.models import model_manifest

                    if not 1024 <= args.port <= 65535:
                        raise ValueError("port must be 1024..65535")
                    if len(args.model) > 1:
                        raise ValueError(
                            "pass one --model for explanations; use --producer-model and --evaluator-model for panels"
                        )
                    models = {p.name: p.resolve() for p in args.model}
                    if args.producer_model or args.evaluator_model:
                        disjoint_panels(
                            [model_manifest(p) for p in args.producer_model],
                            [model_manifest(p) for p in args.evaluator_model],
                        )
                    token = secrets.token_urlsafe(32)
                    print(
                        f"OddsFox: http://127.0.0.1:{args.port}\nSession token (paste into local report): {token}",
                        flush=True,
                    )
                    uvicorn.run(
                        create_app(
                            store,
                            token=token,
                            port=args.port,
                            models=models,
                            auto_sync=not args.no_sync,
                            allow_consensus=args.enable_consensus_publication,
                        ),
                        host="127.0.0.1",
                        port=args.port,
                        workers=1,
                        access_log=False,
                    )
                case "demo":
                    write_json(load_demo(store, args.approve_synthetic))
                case "sync":
                    from oddsfox.catalog import sync_status
                    from oddsfox.sync import SyncRunner

                    runner = SyncRunner(store)
                    try:
                        for venue in args.venue or ["kalshi", "polymarket"]:
                            runner.run_venue(venue, runner.request(venue))
                        write_json(sync_status(store))
                    finally:
                        runner.close()
                case "capture":
                    write_json(fetch(store, args.venue, args.ids))
                case "import":
                    write_json(
                        {
                            "id": import_capture(
                                store,
                                args.venue,
                                args.file.read_bytes(),
                                strict_json(args.documents.read_bytes())
                                if args.documents
                                else None,
                            )
                        }
                    )
                case "draft":
                    if store.get(args.contract_id)["kind"] != "contract":
                        raise ValueError("expected a contract version")
                    write_json(blank_ir(args.contract_id))
                case "interpret":
                    write_json(
                        {
                            "id": pipeline.interpret(
                                args.file.read_bytes(),
                                derivations=strict_json(args.derivations.read_bytes())
                                if args.derivations
                                else None,
                            )
                        }
                    )
                case "register":
                    write_json(
                        {
                            "id": pipeline.register(
                                args.canonical_id,
                                strict_json(args.file.read_bytes()),
                                args.reviewer,
                                args.rationale,
                            )
                        }
                    )
                case "review":
                    write_json(
                        {
                            "id": pipeline.review(
                                args.interpretation_id,
                                args.reviewer,
                                args.rationale,
                                args.approve,
                                args.governing_material_complete,
                            )
                        }
                    )
                case "compile":
                    write_json(
                        {
                            "id": compile_local(
                                store,
                                args.contract_id,
                                args.model_path,
                                constrained=not args.unconstrained,
                                max_tokens=args.max_tokens,
                            )
                        }
                    )
                case "replay":
                    write_json(
                        {
                            "id": replay(
                                store, args.contract_id, args.response_artifact, args.config_id
                            )
                        }
                    )
                case "compare":
                    write_json(pipeline.compare())
                case "publish":
                    write_json({"assertions": pipeline.publish()})
                case "validate":
                    from oddsfox.validation import validate_dataset

                    write_json(
                        validate_dataset(
                            store,
                            args.output,
                            args.producer_model,
                            args.evaluator_model,
                            skip_sync=args.skip_sync,
                        )
                    )
                case "consensus-approve":
                    from oddsfox.consensus import approve_interpretation

                    write_json(
                        {
                            "id": approve_interpretation(
                                store, args.interpretation_id, args.producer_model
                            )
                        }
                    )
                case "status":
                    write_json(store.status())
                case "export":
                    if args.format == "parquet":
                        if not args.output or args.history:
                            raise ValueError(
                                "Parquet export requires --output and contains current accepted assertions"
                            )
                        pipeline.export_parquet(args.output)
                    else:
                        write_json(pipeline.export(args.history), args.output)
                case "backup":
                    store.backup(args.destination)
                    write_json({"backup": str(args.destination)})
        finally:
            store.close()
        return 0
    except (ValueError, OSError, RuntimeError) as exc:
        print(f"oddsfox: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    sys.exit(main())
