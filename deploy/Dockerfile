FROM neo4j:4.4-enterprise

ENV NEO4J_ACCEPT_LICENSE_AGREEMENT=yes

USER root
RUN apt-get update && apt-get install -y curl && \
    mkdir -p /plugins && \
    curl -L -o /plugins/apoc.jar https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases/download/4.4.0.13/apoc-4.4.0.13-all.jar && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER neo4j