pipeline {
  agent any
  stages {

    stage('build') {
      steps {
        sh 'make all'
      }
    }

    stage('test') {
      steps {
        sh 'make test TEST_FAILFAST=1 TEST_COUNT=10'
      }
    }

  }
}
