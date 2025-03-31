# Photos to Excire Exporter (Enhanced: Auto-detect, Album foldering, Concurrent Processing)

import os
import hashlib
import shutil
import json
from pathlib import Path
import subprocess
import plistlib
import sqlite3
from concurrent.futures import ThreadPoolExecutor, as_completed

# CONFIGURABLE
PHOTOS_LIBRARY_ROOT = Path("~/Pictures").expanduser()
EXPORT_BASE_DIR = Path("~/Desktop/Photos_Exported").expanduser()
FAILED_EXPORT_DIR = EXPORT_BASE_DIR / "failedexports"
EXIFTOOL_PATH = "exiftool"  # Assumes exiftool is in PATH
MAX_WORKERS = 8

# GLOBAL LOG
export_log = []


def find_photos_library():
    for item in PHOTOS_LIBRARY_ROOT.iterdir():
        if item.name.endswith(".photoslibrary"):
            return item
    raise FileNotFoundError("No Photos Library found in ~/Pictures")


def checksum_file(file_path):
    hash_sha256 = hashlib.sha256()
    with open(file_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            hash_sha256.update(chunk)
    return hash_sha256.hexdigest()


def extract_photos_with_osxphotos(library_path):
    """Run osxphotos to dump metadata + images."""
    os.makedirs(EXPORT_BASE_DIR, exist_ok=True)
    subprocess.run([
        "osxphotos", "export",
        str(EXPORT_BASE_DIR),
        "--exiftool",
        "--overwrite",
        "--json", "exported.json",
        "--update",
        f"--photos-library", str(library_path)
    ])


def inject_metadata(photo):
    try:
        img_path = Path(photo["path"])
        metadata_args = []

        if "keywords" in photo:
            for keyword in photo["keywords"]:
                metadata_args.extend(["-Keywords=" + keyword])
        if "title" in photo:
            metadata_args.extend(["-Title=" + photo["title"]])
        if "description" in photo:
            metadata_args.extend(["-ImageDescription=" + photo["description"]])
        if "latitude" in photo and "longitude" in photo:
            metadata_args.extend([
                f"-GPSLatitude={photo['latitude']}",
                f"-GPSLongitude={photo['longitude']}"
            ])

        checksum_before = checksum_file(img_path)
        result = subprocess.run([
            EXIFTOOL_PATH,
            *metadata_args,
            str(img_path)
        ], capture_output=True)

        checksum_after = checksum_file(img_path)

        # Foldering by album
        if photo.get("albums"):
            album_name = photo["albums"][0].replace("/", "-")  # sanitize path
            album_dir = EXPORT_BASE_DIR / album_name
            os.makedirs(album_dir, exist_ok=True)
            shutil.move(str(img_path), album_dir / img_path.name)
            img_path = album_dir / img_path.name

        if checksum_before != checksum_after:
            return {
                "status": "success",
                "file": str(img_path),
                "checksum": checksum_after
            }
        else:
            shutil.move(str(img_path), FAILED_EXPORT_DIR / img_path.name)
            return {
                "status": "failed",
                "file": str(img_path),
                "reason": "Checksum unchanged after EXIF inject"
            }
    except Exception as e:
        return {
            "status": "error",
            "file": photo.get("path", "unknown"),
            "reason": str(e)
        }


def wrap_metadata_into_exif():
    with open(EXPORT_BASE_DIR / "exported.json") as f:
        photos = json.load(f)

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(inject_metadata, photo) for photo in photos]
        for future in as_completed(futures):
            result = future.result()
            export_log.append(result)


def write_export_log():
    with open(EXPORT_BASE_DIR / "export-log.json", "w") as f:
        json.dump(export_log, f, indent=2)


def main():
    os.makedirs(FAILED_EXPORT_DIR, exist_ok=True)
    library = find_photos_library()
    extract_photos_with_osxphotos(library)
    wrap_metadata_into_exif()
    write_export_log()


if __name__ == "__main__":
    main()
