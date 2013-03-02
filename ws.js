
var mysql     = require('mysql')
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

/*app.get('/', function(req, res){
  res.send('hello world');
});*/

app.get('/sinal/:dados', function(req, res){
  console.log('Conexão recebida. IP: ' + req.ip);

  var obj = {ip: req.ip, dados: req.params.dados, codigo: geraNovoCodigo()};
  fila.push(obj);

  if (!running) setTimeout(processarSinal, 100);
  res.end();
});

app.get('/json/:equip/:hora_ini/:hora_fim', function(){
  
});

app.listen(porta);
console.log('Escutando a porta ' + porta + '...');

function processarSinal() {
  if (fila.length > 0) {
    if (!running) running = true;
    var atual = fila.shift(); // retorna o primeiro item do array e o remove do array
    console.log('Iniciou processamento do codigo:' + atual.codigo);

    try {
      var sinal;

      try {
         sinal = JSON.parse(new Buffer(atual.dados.replace(/-/g,'+').replace(/_/,'/'), 'base64').toString('utf8'));
      } catch(err) {
        throw "Falha ao fazer parse do JSON enviado ! ERR: " + err;
      }
      var erro = checarDados(sinal);

      if(!!erro) {

        throw erro;

      } else {

      var conn = getConexaoDB();
      conn.connect(function(err) {
        if (!!err) throw "Erro ao conectar com o banco de dados ! ERR: " + err;
      });

      /*conn.query("INSERT INTO GE_TESTE VALUES (?)",[sinal.coord.lng], function(err, rows, fields){
        if (!!err) throw "Erro ao executar comando no banco de dados ! ERR: " + err;
      });*/

      var prc_params = [sinal.imei, sinal.user, sinal.data, sinal.coord.lat, sinal.coord.lng];

      conn.query("call PRC_RECEBE_SINAL(?,?,?,?,?);", prc_params, function(err, rows, fields){
        if (!!err) throw "Erro ao executar comando no banco de dados ! ERR: " + err;
      });

      conn.end();
      }

    } catch (err) {
      console.log('Erro ao processar sinal do codigo: ' + atual.codigo + ' ERR:' + err.message);
    }

    console.log('Finalizou codigo: ' + atual.codigo + '; tamanho da fila restante: ' + fila.length);
    setTimeout(processarSinal, 100);
  } else {
    running = false;
  }
}

function getConexaoDB(){
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
  return Math.ceil(Math.random() * 89 + 10).toString()
       + Math.ceil(Math.random() * 89 + 10).toString()
       + Math.ceil(Math.random() * 89 + 10).toString()
       + Math.ceil(Math.random() * 89 + 10).toString();
}

//url: http://localhost:3014/sinal/eyJpbWVpIjoiMTExMTExMTExIiwidXNlciI6IjEiLCJkYXRhIjoiMjYvMDIvMjAxMyAxODoyMTozNyIsImNvb3JkIjp7ImxhdCI6Ii0yMy4xMjQxMzI0IiwibG5nIjoiLTQ2Ljc2NTg3MiJ9fQ==

//sinal: {"imei":"111111111","user":"1","data":"26/02/2013 18:21:37","coord":{"lat":"-23.1241324","lng":"-46.765872"}}