# OpenRelik PhotoRec Worker

This OpenRelik worker utilizes [PhotoRec](https://www.cgsecurity.org/wiki/PhotoRec) to perform file carving and data recovery from various input sources, typically disk images. It aims to recover deleted files or files from corrupted file systems.

## Features

- Integrates PhotoRec's file recovery capabilities into the OpenRelik processing pipeline.
- Can be configured to search for specific file types.
- Processes input data (e.g., disk images) and outputs recovered files.

## How it Works

The worker receives a task, typically pointing to a disk image or a block device. It then invokes PhotoRec with appropriate parameters to scan the data source. Recovered files are then stored in a designated output location, making them available for further analysis or review within the OpenRelik platform.

## Dependencies
- PhotoRec (must be installed in the worker's environment).
