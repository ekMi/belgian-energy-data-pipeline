
pipeline {
    agent {
        label 'windows-ci'
    }

    options {
        skipDefaultCheckout(true)
    }

    triggers {
        pollSCM('H/5 * * * *')
    }

    stages {
        stage('Checkout') {
            steps {
                bat 'if exist reports\\pytest.xml del /q reports\\pytest.xml'
                checkout scm
                bat 'git rev-parse --short HEAD'
                bat 'whoami'
            }
        }

        stage('Setup Python') {
            steps {
                bat '''
                    "C:\\Program Files\\Python313\\python.exe" -m venv .venv
                    .venv\\Scripts\\python.exe -m pip install -r requirements.txt
                    .venv\\Scripts\\python.exe -m pip check
                '''
            }
        }

        stage('Ruff') {
            steps {
                bat '.venv\\Scripts\\ruff.exe check --no-cache .'
                bat '.venv\\Scripts\\ruff.exe format --check --no-cache .'
            }
        }

        stage('Unit Tests') {
            steps {
                bat '''
                    if not exist reports mkdir reports
                    .venv\\Scripts\\python.exe -B -m pytest tests/unit -p no:cacheprovider -ra --junitxml=reports/pytest.xml
                '''
            }
        }
    }

    post {
         always {
            script {
                if (fileExists('reports/pytest.xml')) {
                    junit testResults: 'reports/pytest.xml'
                } else {
                    echo 'No pytest report generated in this build'
                }
            }
        }
    }
}
