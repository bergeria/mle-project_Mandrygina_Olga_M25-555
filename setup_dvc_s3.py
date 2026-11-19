import os
import subprocess
import sys
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from dotenv import load_dotenv


PROJECT_DIR = Path(__file__).resolve().parent
REMOTE_NAME = "s3_storage"


def run_dvc(*args, cwd=PROJECT_DIR):
    subprocess.run(
        [sys.executable, "-m", "dvc", *args],
        cwd=cwd,
        check=True,
    )


def configure_remote(directory, bucket, endpoint, region):
    run_dvc(
        "remote", "add", "--default", "--force",
        REMOTE_NAME,
        f"s3://{bucket}/mle-project/dvc",
        cwd=directory,
    )
    run_dvc(
        "remote", "modify", REMOTE_NAME,
        "endpointurl", endpoint,
        cwd=directory,
    )
    run_dvc(
        "remote", "modify", REMOTE_NAME,
        "region", region,
        cwd=directory,
    )


def main():
    env_path = PROJECT_DIR / ".env"
    if not env_path.is_file():
        raise FileNotFoundError(f"Не найден файл: {env_path}")

    load_dotenv(env_path, override=True)

    required = [
        "S3_BUCKET_NAME",
        "S3_ENDPOINT_URL",
        "AWS_DEFAULT_REGION",
        "AWS_ACCESS_KEY_ID",
        "AWS_SECRET_ACCESS_KEY",
    ]
    missing = [
        name for name in required
        if not os.getenv(name, "").strip()
    ]
    if missing:
        raise ValueError(
            "Заполните переменные в .env: " + ", ".join(missing)
        )

    bucket = os.environ["S3_BUCKET_NAME"].strip()
    endpoint = os.environ["S3_ENDPOINT_URL"].strip()
    region = os.environ["AWS_DEFAULT_REGION"].strip()

    if not (PROJECT_DIR / ".dvc").is_dir():
        run_dvc("init")

    configure_remote(PROJECT_DIR, bucket, endpoint, region)
    print("\nНастройки S3 сохранены в .dvc/config.", flush=True)

    # Отдельный временный проект для проверки передачи файла.
    with TemporaryDirectory(prefix="mle-dvc-s3-check-") as directory:
        work_dir = Path(directory)

        run_dvc("init", "--no-scm", "--quiet", cwd=work_dir)
        configure_remote(work_dir, bucket, endpoint, region)

        run_dvc(
            "cache", "dir",
            str(work_dir / ".dvc" / "upload-cache"),
            cwd=work_dir,
        )

        test_file = work_dir / "probe.txt"
        payload = f"DVC S3 check: {uuid4().hex}\n".encode("utf-8")
        test_file.write_bytes(payload)

        run_dvc("add", "probe.txt", cwd=work_dir)

        print("\nПроверка записи в S3 через DVC...", flush=True)
        run_dvc(
            "push", "probe.txt.dvc",
            "--remote", REMOTE_NAME,
            cwd=work_dir,
        )

        # Удаляем только временный тестовый файл.
        test_file.unlink()

        # Пустой кеш заставит DVC скачать содержимое из S3.
        run_dvc(
            "cache", "dir",
            str(work_dir / ".dvc" / "download-cache"),
            cwd=work_dir,
        )

        print("\nПроверка чтения из S3 через DVC...", flush=True)
        run_dvc(
            "pull", "probe.txt.dvc",
            "--remote", REMOTE_NAME,
            cwd=work_dir,
        )

        if test_file.read_bytes() != payload:
            raise RuntimeError(
                "Содержимое скачанного файла отличается от исходного."
            )

    print("\nПроверка DVC + S3: успешно.")
    print("Файл загружен в S3 и скачан обратно.")
    print("Содержимое совпадает.")


if __name__ == "__main__":
    main()
