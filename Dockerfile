ARG BASE_IMAGE=python:3.13-bookworm
FROM $BASE_IMAGE

RUN apt-get update \
    && apt-get install -y fuse3 libfuse3-dev pkg-config git xattr \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
COPY . /tmp/

ARG DEV=false
RUN if [ "$DEV" = "false" ]; then pip3 install --no-cache-dir /tmp/; fi
RUN if [ "$DEV" = "true" ]; then cp -fr /tmp /code && pip3 install --no-cache-dir -e /code/[dev]; fi

RUN cp /tmp/bin/start.sh / && chmod +x /start.sh

CMD /start.sh
