FROM rust:1.97.1-slim-bookworm@sha256:2775a09d208ff0d7c1f50490c45b62db929e87ba1dcbc3f2132ac71a704bcdd3 

COPY --from=sclevine/yj /bin/yj /bin/yj
RUN /bin/yj -h
RUN apt-get update && apt-get install llvm-11 psmisc postgresql-contrib postgresql-client ruby ruby-dev libpq-dev python3 python3-pip lcov curl sudo iproute2 -y
RUN cargo install cargo-binutils rustfilt
RUN rustup component add llvm-tools-preview
RUN sudo gem install bundler
RUN wget -O toxiproxy-2.4.0.deb https://github.com/Shopify/toxiproxy/releases/download/v2.4.0/toxiproxy_2.4.0_linux_$(dpkg --print-architecture).deb && \
	sudo dpkg -i toxiproxy-2.4.0.deb
RUN wget -O go1.21.3.linux-$(dpkg --print-architecture).tar.gz https://go.dev/dl/go1.21.3.linux-$(dpkg --print-architecture).tar.gz && \
    sudo tar -C /usr/local -xzf go1.21.3.linux-$(dpkg --print-architecture).tar.gz && \
    rm go1.21.3.linux-$(dpkg --print-architecture).tar.gz
