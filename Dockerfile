FROM prefecthq/prefect:3-latest

WORKDIR /app

# git is required by the deployment's git_clone pull step (clones main per run).
RUN apt-get update && apt-get install -y --no-install-recommends git openssh-client && \
    rm -rf /var/lib/apt/lists/*

# Install project deps first for better layer caching. These deps run the flow;
# the flow's *code* is cloned fresh from git on each run (see prefect.yaml).
COPY requirements.txt ./
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt

# Baked-in copy is used only to register the deployment (`prefect deploy --all`
# reads prefect.yaml). Per-run code comes from the git_clone pull step, not this.
COPY . .

# dbt looks here for profiles.yml (project also passes profiles_dir explicitly).
ENV PREFECT_HOME=/app/.prefect
