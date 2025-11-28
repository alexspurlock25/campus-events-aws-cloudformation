# University of Cincinnati events AWS CDK project

This CDK project is created as a companion to the (unofficial) University of Cincinnati events Android App

Simply what this project does:
1. Grabs all events listed in the RSS feed from UC's events page. (AWS Lambda, EventWatch)
2. Converts the data into a csv format. (S3 bucket)
3. Using an ETL pipeline, it puts new data into a Postgres (Supabase) table. (This is not done yet)
4. Finally, the client (the Android app in our case) fetches the data from the said Postgres table (Kotlin, Jetpack Compose, Groovy)

## Requirements

This project is set up with `poetry` and `pyenv`.

### pyenv
```
pyenv which python
poetry env use $(pyenv which python)
pyenv install 3.9 // if you are missing the version
```

### poetry
```
// this project uses local .venv so you can easliy activate it if you wish
poetry env activate
poetry install
poetry run <command> // pytest for example
```

## Useful commands

 * `[poetry run] cdk ls`          list all stacks in the app
 * `cdk synth`                    emits the synthesized CloudFormation template
 * `cdk deploy`                   deploy this stack to your default AWS account/region
 * `cdk diff`                     compare deployed stack with current state
 * `cdk docs`                     open CDK documentation

## Do you want to contribute?

Email me @ alexanderspurlock@gmail.com
