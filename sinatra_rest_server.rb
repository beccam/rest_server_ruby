require 'sinatra'
require 'JSON'
require 'cassandra'

# connect to the cluster
cluster = Cassandra.cluster
keyspace = 'demo'
session  = cluster.connect(keyspace)

allUserSelectStatement = session.prepare("SELECT firstname,lastname, age, email, city FROM users")
userSelectStatement = session.prepare("SELECT firstname,lastname, age, email, city FROM users where lastname = ?")
userInsertStatement = session.prepare("INSERT INTO users (firstname, lastname, age, city, email) VALUES (?,?,?,?,?)")
userUpdateStatement = session.prepare("UPDATE users SET age = ? WHERE lastname = ?")
userDeleteStatement = session.prepare("DELETE FROM users WHERE lastname = ?")

# TODO Put exception handling around each method

# Get one user
get '/users/:lastname' do

  result = session.execute(userSelectStatement, :arguments [params[:lastname]])

  if result.size < 1
    halt(404)
  end

  result.first.to_json

end

# Insert a user
post '/users' do

  session.execute(userInsertStatement, :arguments=>[params[:firstname],params[:lastname],params[:age].to_i,params[:city],params[:email]])

  "Inserted"

end

# Update a user
put '/users' do

  session.execute(userUpdateStatement, :arguments=>[params[:age].to_i,params[:lastname]])

  "Updated"

end

# Delete a user
delete '/users/:lastname' do

  session.execute(userDeleteStatement, :arguments=>[params[:lastname]])

  "Deleted"

end
