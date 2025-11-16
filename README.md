# Welcome to your CDK Python project!
## Requirements

### pyenv
```
pyenv install 3.9
pyenv local 3.9.x
pyenv which python
poetry env use $(pyenv which python)
```

### poetry
```
poetry env activate
poetry install
poetry run <command> // pytest
```

This is a blank project for CDK development with Python.

The `cdk.json` file tells the CDK Toolkit how to execute your app.

This project is set up with `poetry` and `pyenv`.

## Useful commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation
