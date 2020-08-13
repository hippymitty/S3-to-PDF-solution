# Reporting Lambda

Pre-requisites:
- NPM

How to prepare:
```bash
# Install NPM and Serverless Framework
npm install -g serverless

# Install the relevant plugins 
sls plugin install --name serverless-python-requirements
npm install serverless-plugin-existing-s3
```

## Deploy

```bash
sls deploy --stage <dev|test|prod> --aws-profile <<YOUR_PROFILE_HERE>>
```

## Run local tests
pip3 install -r requirements.txt
python3 -m pytest test
