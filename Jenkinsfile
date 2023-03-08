#!groovy

def tryStep(String message, Closure block, Closure tearDown = null) {
    try {
        block()
    }
    catch (Throwable t) {
        slackSend message: "${env.JOB_NAME}: ${message} failure ${env.BUILD_URL}", channel: '#ci-channel', color: 'danger'

        throw t
    }
    finally {
        if (tearDown) {
            tearDown()
        }
    }
}


node('GOBBUILD') {
    withEnv(["DOCKER_IMAGE_NAME=datapunt/gob_prepare:${env.BUILD_NUMBER}",
             "BENK_ACR_ONTW=benkweuacrofkl2hn5eivwy.azurecr.io"
            ]) {

        stage("Checkout") {
            checkout scm
        }

        stage('Test') {
            lock("gob-prepare-test") {
                tryStep "test", {
                    sh "docker-compose -p gob_prepare -f src/.jenkins/test/docker-compose.yml build --no-cache && " +
                       "docker-compose -p gob_prepare -f src/.jenkins/test/docker-compose.yml run --rm test"

                }, {
                    sh "docker-compose -p gob_prepare -f src/.jenkins/test/docker-compose.yml down"
                }
            }
        }

        stage("Build image") {
            tryStep "build", {
                docker.withRegistry("${DOCKER_REGISTRY_HOST}",'docker_registry_auth') {
                    def image = docker.build("${DOCKER_IMAGE_NAME}",
                        "--no-cache " +
                        "--shm-size 1G " +
                        "--build-arg BUILD_ENV=acc " +
                        "--target application " +
                        " src")
                    image.push()
                }
            }
        }

        String BRANCH = "${env.BRANCH_NAME}"

        if (BRANCH == "develop") {

            stage("Push develop image to ACR") {
                tryStep "image tagging", {
                    // Create credentials for the ACR:
                    // az acr token create --registry=<registry_name> --name=jenkins-build-token --scope-map=_repositories_push
                    withCredentials([usernamePassword(credentialsId: 'BENK_ONTW_ACR_JENKINS', usernameVariable: 'ACR_USERNAME', passwordVariable: 'ACR_TOKEN')]) {
                        echo "Push image to ${ACR_USERNAME}@${BENK_ACR_ONTW}"
                        docker.withRegistry("https://${BENK_ACR_ONTW}", 'BENK_ONTW_ACR_JENKINS') {
                            def image = docker.image("${DOCKER_IMAGE_NAME}")
                            image.push("develop")
                            image.push("test")
                        }
                    }
                }
            }

            stage('Push develop image') {
                tryStep "image tagging", {
                    docker.withRegistry("${DOCKER_REGISTRY_HOST}",'docker_registry_auth') {
                       def image = docker.image("${DOCKER_IMAGE_NAME}")
                       image.pull()
                       image.push("develop")
                       image.push("test")
                    }
                }
            }

            stage("Deploy to TEST") {
                tryStep "deployment", {
                    build job: 'Subtask_Openstack_Playbook',
                        parameters: [
                            [$class: 'StringParameterValue', name: 'INVENTORY', value: 'test'],
                            [$class: 'StringParameterValue', name: 'PLAYBOOK', value: 'deploy.yml'],
                            [$class: 'StringParameterValue', name: 'PLAYBOOKPARAMS', value: "-e cmdb_id=app_gob-prepare"],
                        ]
                }
            }

        }

        if (BRANCH == "master") {

            stage('Push acceptance image') {
                tryStep "image tagging", {
                    docker.withRegistry("${DOCKER_REGISTRY_HOST}",'docker_registry_auth') {
                        def image = docker.image("${DOCKER_IMAGE_NAME}")
                        image.pull()
                        image.push("acceptance")
                    }
                }
            }

            stage("Deploy to ACC") {
                tryStep "deployment", {
                    build job: 'Subtask_Openstack_Playbook',
                        parameters: [
                            [$class: 'StringParameterValue', name: 'INVENTORY', value: 'acceptance'],
                            [$class: 'StringParameterValue', name: 'PLAYBOOK', value: 'deploy.yml'],
                            [$class: 'StringParameterValue', name: 'PLAYBOOKPARAMS', value: "-e cmdb_id=app_gob-prepare"],
                        ]
                }
            }
        }
    }
}
