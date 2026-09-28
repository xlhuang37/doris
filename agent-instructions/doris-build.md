# Build the Doris backend

Doris can only be built in a Docker container. The checkout this repository
expects is `/home/ubuntu/nvme/doris` on the host, which is `/root/nvme/doris`
inside the container.

## Owner-provided interactive recipe

To enter the docker environment, run on the host:

```bash
sudo docker run -it --network=host --rm \
  -v "$HOME/nvme:/root/nvme" \
  -v "$HOME/.ssh:/root/.ssh:ro" \
  apache/doris:build-env-ldb-toolchain-latest
```

Inside the container, if `/root/nvme/doris` does not already exist:

```bash
cd /root/nvme
git clone git@github.com:xlhuang37/doris.git
```

Then build the existing or newly cloned checkout:

```bash
cd /root/nvme/doris
bash build.sh --be
```

## Noninteractive build for agents

For an existing checkout, the helper runs the backend build without allocating a
terminal or mounting SSH credentials:

```bash
~/agent-workspace/scripts/build-doris.sh --dry-run
~/agent-workspace/scripts/build-doris.sh
```

Essentially, the script runs the container non-interactively by running pre-specified commands.
The dry run prints the `docker run` invocation, which builds with `bash build.sh --be`
in `/root/nvme/doris` using `apache/doris:build-env-ldb-toolchain-latest`.
