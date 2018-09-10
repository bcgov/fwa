node ('master'){
  stage ('Deploy') {
    checkout scm
    sh 'npm install'
    sh 'npm run build-demo'
    sh 'rm -rf /var/www/bcgov.revolsys.com/htdocs/fwaDemo/*'
    sh 'cp -r dist/* -r /var/www/bcgov.revolsys.com/htdocs/fwaDemo/'
  }
}
