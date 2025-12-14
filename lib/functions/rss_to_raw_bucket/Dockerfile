FROM public.ecr.aws/lambda/python:3.14

# Update system and pip
RUN microdnf update -y && \
    microdnf clean all && \
    python3 -m pip install --upgrade pip setuptools wheel

# Copy and install to /asset (required by CDK)
WORKDIR /asset
COPY requirements.txt .
RUN pip install -r requirements.txt -t /asset

# Copy function code
COPY . /asset
