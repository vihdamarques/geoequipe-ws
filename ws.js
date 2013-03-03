
var mysql     = require('mysql')
   ,async     = require('async')
   ,express   = require('express')
   ,app       = express()
   ,DBConfigs = {
                  host: 'localhost'
                 ,port: 3306
                 ,user: 'geuser'
                 ,password: 'metodista2013'
                 ,database: 'geoequipe'
                }
   ,fila      = []
   ,running   = false
   ,porta     = 3014;

// Recepção de dados
app.get('/sinal/:dados', function(req, res){
  console.log('Conexão recebida. IP: ' + req.ip);

  var obj = {ip: req.ip, dados: req.params.dados, codigo: geraNovoCodigo()};
  fila.push(obj);

  if (!running) setTimeout(processarSinal, 100);
  res.end();
});

// Webservice de dados
app.get('/json/:equip/:hora_ini/:hora_fim', function(){
  
});

app.listen(porta);
console.log('Escutando a porta ' + porta + '...');

function processarSinal() {
  if (fila.length > 0) {
    if (!running) running = true;
    // retorna o primeiro item do array e o remove do array
    var atual = fila.shift();
    console.log(atual.codigo + ' - Iniciou');

    try {
      var sinal, v_id_usuario, v_id_equipamento, erro, conn, params;

      // tenta converter o parametro em base64 da url, converter para texto e depois para JSON
      try {
         sinal = JSON.parse(new Buffer(atual.dados.replace(/-/g,'+').replace(/_/,'/'), 'base64').toString('utf8'));
      } catch(err) {
        throw "Falha ao fazer parse do JSON enviado ! ERR: " + err;
      }
      erro = checarDados(sinal);

      if(!!erro) {
        throw erro;
      } else {
        async.series([
          function(callback) { // conectar DB
            conn = getConexaoDB();
            conn.connect(function(err) {
              if (!!err) callback("Erro ao conectar com o banco de dados ! ERR: " + err);
              //conn.on('error',function(err){console.log(err.code)});
              callback(null,'conectou');
            });
          }
         ,function(callback) { // Verifica se o usuário existe
            conn.query("select id_usuario from ge_usuario where id_usuario = ?", sinal.user, function(err, rows, fields) {
              
              if (!!err) callback("Erro ao verificar usuario ! ERR: " + err);
              if (rows.length == 0)
                callback("Usuario " + sinal.user + " nao encontrado!");
              else
                v_id_usuario = rows[0].id_usuario;
              callback(null,'checou usuario');
            });
          }
         ,function(callback) { // Verifica se o equipamento do imei recebido existe
            conn.query("select id_equipamento from ge_equipamento where imei = ?;", sinal.imei, function(err, rows, fields) {
              if (!!err) callback("Erro ao verificar equipamento ! ERR: " + err);
              if (rows.length == 0)
                callback("Equipamento " + sinal.imei + " nao encontrado!");
              else
                v_id_equipamento = rows[0].id_equipamento;
              callback(null,'checou equipamento');
            });
          }
         ,function(callback) { // Insere sinal
            // Parametros que serao inseridos
            params = [v_id_usuario
                     ,v_id_equipamento
                     ,sinal.data
                     ,sinal.coord.lat
                     ,sinal.coord.lng
                     ,sinal.coord.lng
                     ,sinal.coord.lat
                     ];
            conn.query("insert into ge_sinal (id_usuario,id_equipamento,data_sinal,data_servidor,latitude,longitude,coordenada) "
                     + "values (?,?,str_to_date(?,'%d/%m/%Y %H:%i:%S'),now(),?,?,point(?,?));"
                      ,params, function(err, rows, fields) {
              if (!!err) callback("Erro ao inserir sinal ! ERR: " + err);
              callback(null,'inseriu sinal');
            });
          }
         ,function(callback) { // Finaliza conexao
            conn.end();
            callback(null, 'desconectou');
          }
        ], function(err, results) { // Tratamento de erros
             if (!!err) console.log(atual.codigo + ' - Erro: ' + err);
             console.log(atual.codigo + ' - Finalizou; tamanho da fila restante: ' + fila.length);
             setTimeout(processarSinal, 100);
        });

      }

    } catch (err) {
      console.log(atual.codigo + ' - Erro:' + err.message);
    }
  } else {
    running = false;
  }
}

function getConexaoDB() {
  return mysql.createConnection(DBConfigs);
}

// Validação dos dados recebidos
function checarDados(sinal) {
  if (!sinal.imei) {
    return "IMEI não localizado!";
  }

  if (!sinal.user) {
    return "Usuário não localizado!";
  }

  if (!sinal.data) {
    return "Longitude não localizada!";
  }

  if (!sinal.coord) {
    return "Coordenadas não localizadas!";
  }

  if (!sinal.coord.lat) {
    return "Latitude não localizada!";
  }

  if (!sinal.coord.lng) {
    return "Longitude não localizada!";
  }

  return null;
}

function geraNovoCodigo() {
  return Math.ceil(Math.random() * 9999999).toString();
}

//url: http://localhost:3014/sinal/eyJpbWVpIjoiMTExMTExMTExIiwidXNlciI6IjEiLCJkYXRhIjoiMjYvMDIvMjAxMyAxODoyMTozNyIsImNvb3JkIjp7ImxhdCI6Ii0yMy4xMjQxMzI0IiwibG5nIjoiLTQ2Ljc2NTg3MiJ9fQ==

//sinal: {"imei":"111111111","user":"1","data":"26/02/2013 18:21:37","coord":{"lat":"-23.1241324","lng":"-46.765872"}}