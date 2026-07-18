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
            // The golang container's default user has no writable HOME, so Go
            // can't create its build/module cache. Point HOME at the workspace,
            // which IS writable, so caches land in ./ (throwaway) instead of /.
            environment {
                HOME = "${WORKSPACE}"
            }
            steps {
                sh 'go vet ./...'
                sh 'go build ./...'
            }
        }

        // Build a Docker image of ForgeFlow (using the repo's Dockerfile) and
        // push it to Docker Hub. Runs on the Jenkins node (agent any), which now
        // has Docker access.
        stage('Build & Push Image') {
            steps {
                script {
                    // Tag with the Jenkins build number, e.g. shreesha001/forgeflow:7
                    def img = docker.build("shreesha001/forgeflow:${BUILD_NUMBER}")

                    // Log in to Docker Hub using the 'dockerhub' credential, then push.
                    docker.withRegistry('https://index.docker.io/v1/', 'dockerhub') {
                        img.push()            // push the :BUILD_NUMBER tag
                        img.push('latest')    // also update :latest
                    }
                }
            }
        }
    }
}
