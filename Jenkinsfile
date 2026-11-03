
pipeline {
    agent any

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Verify Repository') {
            steps {
                bat 'git rev-parse --short HEAD'
                bat 'git status --short'
                bat 'dir'
            }
        }

        stage('Verify Python') {
            steps {
                bat '"C:\\Program Files\\Python313\\python.exe" --version'
            }
        }
    }
}
