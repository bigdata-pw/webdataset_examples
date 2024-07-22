import os
import pathlib
import tqdm

from concurrent.futures import ProcessPoolExecutor, as_completed
import webdataset as wds

"""
Webdataset ShardWriter multi-processing example using ProcessPoolExecutor

This example is for a images+metadata dataset where:
- Metadata, `items` in the example, is a list from e.g. JSON, MongoDB etc.
- Filenames for each item are not in the metadata and need to be selected based on `id` field
    - Multiple files for each item
"""

items = list(ITEMS.find({}, {"_id": 0}))
# items = json.load(...)

BASE = pathlib.Path("/bigdata/my_dataset")
IMAGE_BASE = BASE / "images"
SHARD_BASE = BASE / "shards"
SHARD_BASE.mkdir(exist_ok=True)

# listing files with os.listdir is a lot faster than pathlib glob
# especially with large directories
files = os.listdir(IMAGE_BASE)


def get_item(item):
    """
    Items in this example have `id` field.
    Filenames are in format `{id}_N.jpg`
    In this example the output for files will be:
    `1.jpg`, `2.jpg`, `3.jpg`, ...
    """
    item_files = sorted(
        list(filter(lambda filename: filename.split("_")[0] == item["id"], files))
    )
    item_files = {
        f"{idx+1}.jpg": (IMAGE_BASE / file).read_bytes()
        for idx, file in enumerate(item_files)
    }
    item = {"__key__": item["id"], "json": item}
    item.update(item_files)
    return item


"""
General notes:
    `maxcount` default is `10000` and `maxsize` default is `3e9` or 3GB
    3GB is a good choice because there can be issues uploading files larger than 5GB to S3 etc.
    However if 10000 items exceeds 3GB the shard will have with fewer files than expected.
    It's important to check in case of later relying on the expected count to be in each shard.

    There can be diminishing returns on `max_workers`, adjust this based on disk type and cpu count.
"""
with wds.ShardWriter(
    f"{str(SHARD_BASE)}/my_dataset-%05d.tar", maxcount=1000, maxsize=3e9
) as sink:
    """
    A batch is taken from items (10000 in this case) and submitted to the executor.
        Batches are used because submitting many futures can take some time and consume a lot of memory.
        The batch is removed from items.
    Items are added to `results` as futures complete.
    When results is larger than N (1000 in this case) the items in `results` are passed to ShardWriter.
    Finally any remaining results are passed to ShardWriter.

    For loop can be used like
    batch_size = 10000
    for n in range(0, len(items), batch_size):
        futures = {}
        batch = items[n:n+batch_size]
        ...
    """
    results = []
    with ProcessPoolExecutor(max_workers=16) as executor:
        while items:
            futures = {}
            batch = items[:10000]
            for item in tqdm.tqdm(batch, desc="item futures"):
                futures[executor.submit(get_item, item)] = item
            del items[:10000]
            for future in tqdm.tqdm(
                as_completed(futures), desc="completed futures", total=len(futures)
            ):
                result = future.result()
                results.append(result)
                if len(results) > 1000:
                    for result in results:
                        sink.write(result)
                    results = []
        for result in results:
            sink.write(result)
