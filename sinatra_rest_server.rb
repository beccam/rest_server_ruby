require 'sinatra'
require 'JSON'
require 'cassandra'
require 'logger'

log = Logger.new(STDOUT)
log.level = Logger::INFO

# connect to the cluster
cluster = Cassandra.cluster(logger: log)
keyspace = 'demo'
session  = cluster.connect(keyspace)

allUserSelectStatement = session.prepare("SELECT firstname,lastname, age, email, city FROM users")
userSelectStatement = session.prepare("SELECT firstname,lastname, age, email, city FROM users where lastname = ?")
userInsertStatement = session.prepare("INSERT INTO users (firstname, lastname, age, city, email) VALUES (?,?,?,?,?)")
userUpdateStatement = session.prepare("UPDATE users SET age = ? WHERE lastname = ?")
userDeleteStatement = session.prepare("DELETE FROM users WHERE lastname = ?")


# Get one user
get '/users/:lastname' do

  begin
    result = session.execute(userSelectStatement, :arguments=>[params[:lastname]])

    if result.size < 1
      halt(404)
    end

    result.first.to_json
  rescue Exception => e
    log.error 'Error in select a user'
    log.error(e)
    halt(404)
  end

end

# Insert a user
post '/users' do

  begin
    session.execute(userInsertStatement, :arguments=>[params[:firstname],params[:lastname],params[:age].to_i,params[:city],params[:email]])

    "Inserted"

  rescue Exception => e
    log.error 'Error in insert a user'
    log.error(e)
    halt(404)
  end

end

# Update a user
put '/users' do

  begin
    session.execute(userUpdateStatement, :arguments=>[params[:age].to_i,params[:lastname]])

    "Updated"

  rescue Exception => e
    log.error 'Error in update a user'
    log.error(e)
    halt(404)
  end

end

# Delete a user
delete '/users/:lastname' do

  begin
    session.execute(userDeleteStatement, :arguments=>[params[:lastname]])

    "Deleted"

  rescue Exception => e
    log.error 'Error in delete a user'
    log.error(e)
    halt(404)
  end

end
