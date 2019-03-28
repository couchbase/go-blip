pipeline {
    // Build on this uberjenkins node, as it has the Go environment set up in a known-state
    // We could potentially change this to use a dockerfile agent instead so it can be portable.
    agent { label 'sync-gateway-pipeline-builder' }

    environment {
        GO_VERSION = 'go1.11.5'
        GVM = "/root/.gvm/bin/gvm"
        GO = "/root/.gvm/gos/${GO_VERSION}/bin"
        GOPATH = "${WORKSPACE}/gopath"
    }

    stages {
        stage('Setup') {
            parallel {
                stage('Bootstrap') {
                    steps {
                        // Move the automatically checked out code into the right GOPATH structure
                        sh 'mkdir -p $GOPATH/src/github.com/couchbase/go-blip'
                        sh 'find . -maxdepth 1 -mindepth 1 -not -name gopath -exec mv \'{}\' $GOPATH/src/github.com/couchbase/go-blip/ \\;'
                    }
                }

                stage('Go') {
                    stages {
                        stage('Go') {
                            steps {
                                echo 'Installing Go via gvm..'
                                // We'll use Go 1.10.4 to bootstrap compilation of newer Go versions
                                // (because we know this version is installed on the Jenkins node)
                                withEnv(["GOROOT_BOOTSTRAP=/root/.gvm/gos/go1.10.4"]) {
                                    // Use gvm to install the required Go version, if not already
                                    sh "${GVM} install $GO_VERSION"
                                }
                            }
                        }
                        stage('Tools') {
                            steps {
                                withEnv(["PATH+=${GO}"]) {
                                    sh "go version"
                                    // cover is used for building HTML reports of coverprofiles
                                    sh 'go get -v golang.org/x/tools/cmd/cover'
                                    // Jenkins coverage reporting tools
                                    sh 'go get -v github.com/axw/gocov/...'
                                    sh 'go get -v github.com/AlekSi/gocov-xml'
                                }
                            }
                        }
                    }
                }
            }
        }

        stage('Build') {
            steps {
                withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                    sh 'go get -v github.com/couchbase/go-blip'
                    sh 'go build -v github.com/couchbase/go-blip'
                }
            }
        }

        stage('Test with -race -cover') {
            steps {
                withEnv(["PATH+=${GO}:${GOPATH}/bin"]) {
                    sh 'go get -t -v github.com/couchbase/go-blip'
                    sh "go test -v -race -timeout 30m -coverprofile=cover.out github.com/couchbase/go-blip"
                    sh 'go tool cover -func=cover.out | awk \'END{print "Total Coverage: " $3}\''

                    sh 'mkdir -p reports'
                    sh 'go tool cover -html=cover.out -o reports/coverage.html'

                    // Generate Cobertura XML report that can be parsed by the Jenkins Cobertura Plugin
                    sh 'gocov convert cover.out | gocov-xml > reports/coverage.xml'
                }
            }
        }

    }

    post {
        always {
            // Publish the HTML test coverage reports we generated
            publishHTML([allowMissing: false, alwaysLinkToLastBuild: false, includes: 'coverage.html', keepAll: false, reportDir: 'reports', reportFiles: '*.html', reportName: 'Code Coverage', reportTitles: ''])

            // Publish the cobertura formatted test coverage reports into Jenkins
            cobertura autoUpdateHealth: false, autoUpdateStability: false, coberturaReportFile: 'reports/coverage.xml', conditionalCoverageTargets: '70, 0, 0', failNoReports: false, failUnhealthy: false, failUnstable: false, lineCoverageTargets: '80, 0, 0', maxNumberOfBuilds: 0, methodCoverageTargets: '80, 0, 0', sourceEncoding: 'ASCII', zoomCoverageChart: false

            // Publish the junit test reports
            // junit allowEmptyResults: true, testResults: 'reports/test-*.xml'

            step([$class: 'WsCleanup'])
        }
    }
}
