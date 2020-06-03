pipeline {
  agent any
  stages {
    stage('install golang') {
      parallel {
        stage('install golang') {
          steps {
            sh 'make golang'
          }
        }

        stage('install protobuf') {
          steps {
            sh 'make protobuf'
          }
        }

      }
    }

    stage('install gogoproto') {
      steps {
        sh 'make gogoproto'
      }
    }

    stage('build') {
      steps {
        sh 'make all'
      }
    }

    stage('test') {
      steps {
        sh 'make test'
      }
    }

  }
}
