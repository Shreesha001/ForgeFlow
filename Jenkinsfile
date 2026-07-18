// ForgeFlow CI/CD pipeline.
// Jenkins reads this file straight from the repo ("Pipeline script from SCM").
// With SCM mode, Jenkins checks out the code automatically before running the
// stages below, so we do NOT need a manual `git` step here.

pipeline {
    // Where the stages run. "any" = on the Jenkins machine itself.
    agent any

    stages {

        // Sanity check: prove Jenkins can reach Docker.
        // Once this passes, every later stage can build/run containers.
        stage('Check Docker') {
            steps {
                sh 'docker version'
            }
        }

        // Build and vet the Go code.
        // The Jenkins container has no Go installed, so instead of installing it
        // we borrow the official golang image: this stage runs INSIDE a
        // temporary golang:1.26 container, which already has the Go toolchain.
        stage('Vet & Build') {
            agent {
                docker {
                    image 'golang:1.26'
                    reuseNode true // keep using the same checked-out code
                }
            }
            steps {
                sh 'go vet ./...'
                sh 'go build ./...'
            }
        }
    }
}
