pipeline {
    agent { label 'windows-ci' }

    options {
        skipDefaultCheckout(true)
        disableConcurrentBuilds()
    }

    triggers {
        pollSCM('H/5 * * * *')
    }

    stages {
        stage('Checkout') {
            steps {
                // Clear stale reports even if checkout or setup subsequently fails.
                dir('reports') { deleteDir() }
                checkout scm
                bat 'git rev-parse --short HEAD'
                bat 'whoami'
            }
        }

        stage('Setup Python') {
            steps {
                bat 'powershell -NoProfile -ExecutionPolicy Bypass -File ci\\setup_env.ps1 -Environment .venv -Requirements requirements.txt'
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
                // Prefect tests are explicitly assigned to their own environment.
                bat '.venv\\Scripts\\python.exe -B ci\\run_tests.py unit'
            }
        }

        stage('Prefect Tests') {
            steps {
                bat 'powershell -NoProfile -ExecutionPolicy Bypass -File ci\\setup_env.ps1 -Environment .venv-prefect -Requirements requirements-prefect.txt'
                bat 'powershell -NoProfile -ExecutionPolicy Bypass -File ci\\ensure_sqlalchemy_pure.ps1 -Environment .venv-prefect'
                bat '.venv-prefect\\Scripts\\python.exe -B ci\\run_tests.py prefect'
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
                        bat '@echo off\r\n.venv\\Scripts\\python.exe -B ci\\run_tests.py integration'
                    }
                }
            }
        }

        stage('dbt Parse') {
            steps {
                bat 'powershell -NoProfile -ExecutionPolicy Bypass -File ci\\setup_env.ps1 -Environment .venv-dbt -Requirements dbt\\requirements.txt'
                withEnv([
                    'PGHOST=127.0.0.1',
                    'PGPORT=5432',
                    'PGDATABASE=ci_parse_only',
                    'PGUSER=ci_parse_only',
                    'PGPASSWORD=ci_placeholder_not_a_secret',
                    'DBT_SEND_ANONYMOUS_USAGE_STATS=false'
                ]) {
                    bat 'mkdir reports\\dbt-profile'
                    bat 'copy /Y dbt\\profiles.yml.example reports\\dbt-profile\\profiles.yml >NUL'
                    bat '.venv-dbt\\Scripts\\dbt.exe parse --project-dir dbt --profiles-dir reports\\dbt-profile --target-path "%WORKSPACE%\\reports\\dbt-parse" --log-path "%WORKSPACE%\\reports\\dbt-parse-logs" --no-partial-parse'
                }
            }
        }

        stage('dbt Build & Tests') {
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
                        bat '@echo off\r\n.venv\\Scripts\\python.exe -B -m ci.dbt_build'
                    }
                }
            }
        }
    }

    post {
        always {
            script {
                try {
                    if (fileExists('reports/unit.xml') ||
                        fileExists('reports/prefect.xml') ||
                        fileExists('reports/integration.xml') ||
                        fileExists('reports/dbt.xml')) {
                        junit testResults: 'reports/*.xml'
                    } else {
                        echo 'No test reports generated in this build'
                    }
                } finally {
                    archiveArtifacts artifacts: 'reports/**', allowEmptyArchive: true
                }
            }
        }
    }
}
