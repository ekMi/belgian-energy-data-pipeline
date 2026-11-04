
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
                bat 'if exist reports\\integration.xml del /q reports\\integration.xml'
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
        stage('Integration Tests') {
            steps {
                withCredentials([usernamePassword(
                    credentialsId: 'belgian-energy-postgres-test',
                    usernameVariable: 'TEST_PGUSER',
                    passwordVariable: 'TEST_PGPASSWORD'
                )]) {
                    withEnv([
                        'TEST_PGHOST=127.0.0.1',
                        'TEST_PGPORT=5432',
                        'TEST_PGDATABASE=belgian_energy_test'
                    ]) {
                        bat '''@echo off
                            .venv\\Scripts\\python.exe -c "import os,sys; sys.exit(0 if (os.environ.get('TEST_PGDATABASE') == 'belgian_energy_test' and os.environ.get('TEST_PGUSER') == 'energy_test_app' and os.environ.get('TEST_PGPASSWORD')) else 1)"
                        '''

                        bat '''@echo off
                            .venv\\Scripts\\python.exe -B -m pytest tests/integration -p no:cacheprovider -m integration -ra --junitxml=reports/integration.xml
                        '''

                        bat '''@echo off
                            .venv\\Scripts\\python.exe -c "import sys,xml.etree.ElementTree as ET; r=ET.parse('reports/integration.xml').getroot(); suites=[r] if r.tag=='testsuite' else r.findall('testsuite'); count=sum(int(s.get('tests',0)) for s in suites); skipped=sum(int(s.get('skipped',0)) for s in suites); print('Integration tests:',count,'skipped:',skipped); sys.exit(0 if count>0 and skipped==0 else 1)"
                        '''
                    }
                }
            }
        }
        stage('dbt Parse') {
            steps {
                withEnv([
                    'PGHOST=127.0.0.1',
                    'PGPORT=5432',
                    'PGDATABASE=ci_parse_only',
                    'PGUSER=ci_parse_only',
                    'PGPASSWORD=ci_placeholder_not_a_secret'
                ]) {
                    bat '''@echo off
                        "C:\\Program Files\\Python313\\python.exe" -m venv .venv-dbt
                        .venv-dbt\\Scripts\\python.exe -m pip install -r dbt\\requirements.txt
                        .venv-dbt\\Scripts\\python.exe -m pip check
                        copy /Y dbt\\profiles.yml.example dbt\\profiles.yml >NUL
                        .venv-dbt\\Scripts\\dbt.exe parse --project-dir dbt --profiles-dir dbt
                    '''
                }
            }
        }
    }

    post {
        always {
            script {
                if (fileExists('reports/pytest.xml') ||
                    fileExists('reports/integration.xml')) {
                    junit testResults: 'reports/*.xml'
                } else {
                    echo 'No test reports generated in this build'
                }
            }
        }
    }
}
