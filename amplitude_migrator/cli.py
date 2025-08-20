import argparse, importlib.util, os, sys
from .runner import run_migration

def load_config_module(path: str):
    spec = importlib.util.spec_from_file_location("user_config", path)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)  # type: ignore
    return mod

def config_to_dict(mod) -> dict:
    # pull UPPERCASE names only
    return {k: getattr(mod, k) for k in dir(mod) if k.isupper()}

def main():
    p = argparse.ArgumentParser(description="Amplitude migration runner")
    p.add_argument("--config", default="config.py", help="Path to config.py")
    p.add_argument("--dry-run", action="store_true", help="Override DRY_RUN=True")
    args = p.parse_args()

    if not os.path.exists(args.config):
        print(f"Config file not found: {args.config}", file=sys.stderr)
        sys.exit(1)

    mod = load_config_module(args.config)
    cfg = config_to_dict(mod)
    if args.dry_run:
        cfg["DRY_RUN"] = True

    run_migration(cfg)
