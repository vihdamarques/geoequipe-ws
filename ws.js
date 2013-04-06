var mysql     = require('mysql')
   ,async     = require('async')
   ,http      = require('http')
   ,express   = require('express')
   ,app       = express()
   ,DBConfigs = {
                  host: 'localhost'
                 ,port: 3306
                 ,user: 'geuser'
                 ,password: 'metodista2013'
                 ,database: 'geoequipe'
                 ,multipleStatements: true
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

// Verifica endereços para geolocalização
setTimeout(verificaEndereco, 100);

function processarSinal() {
  if (fila.length > 0) {
    if (!running) running = true;
    // retorna o primeiro item do array e o remove do array
    var atual = fila.shift();
    console.log(atual.codigo + ' - Iniciou');

    try {
      var sinal, v_id_usuario, v_id_equipamento, erro, conn;

      // tenta converter o parametro em base64 da url, converter para string e depois para JSON
      try {
         sinal = JSON.parse(new Buffer(atual.dados.replace(/-/g,'+').replace(/_/,'/'), 'base64').toString('utf8'));
      } catch (err) {
        throw "Falha ao fazer parse do JSON enviado ! ERR: " + err.message;
      }
      erro = checarDados(sinal); // valida se todas as informações foram preenchidas

      if(!!erro) {
        throw erro;
      } else {
        // controla os processos assicronos
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
            conn.query("select id_equipamento from ge_equipamento where imei = ?", sinal.imei, function(err, rows, fields) {
              if (!!err) callback("Erro ao verificar equipamento ! ERR: " + err);
              if (rows.length == 0)
                callback("Equipamento " + sinal.imei + " nao encontrado!");
              else
                v_id_equipamento = rows[0].id_equipamento;
              callback(null,'checou equipamento');
            });
          }
         ,function(callback) { // Verifica ultimo sinal
            conn.query("select s.latitude, s.longitude, s.id_sinal "
                     + "from ge_sinal s, ge_usuario u "
                     + "where s.id_sinal = u.id_ultimo_sinal and u.id_usuario = ?", sinal.user, function(err, rows, fields) {
              if (!!err) callback("Erro ao verificar ultimo sinal ! ERR: " + err);
              if (rows.length == 0)
                callback(null,'nao possui ultimo sinal');
              else {
                sinal.ultimoSinal = {id_sinal: rows[0].id_sinal, lat: rows[0].latitude, lng: rows[0].longitude};
                callback(null,'pegou ultimo sinal');
              }
            });
          }
         ,function(callback) { // Insere sinal
            var dist;
            if (!!sinal.ultimoSinal) dist = distancia(sinal.coord.lat, sinal.coord.lng, sinal.ultimoSinal.lat, sinal.ultimoSinal.lng);
            // Grava se distancia for maior que 20 metros do ultimo ponto ou nao existir ultimo ponto
            if (dist === undefined || dist > 20) {
              conn.query("insert into ge_sinal (id_usuario,id_equipamento,data_sinal,data_servidor,latitude,longitude,coordenada) "
                       + "values (?,?,str_to_date(?,'%d/%m/%Y %H:%i:%S'),now(),?,?,point(?,?))"
                        ,[v_id_usuario,v_id_equipamento,sinal.data,sinal.coord.lat,sinal.coord.lng,sinal.coord.lng,sinal.coord.lat]
                        ,function(err, rows, fields) {
                if (!!err) callback("Erro ao inserir sinal ! ERR: " + err);
                sinal.id_sinal = rows.insertId;
                callback(null,"inseriu sinal");
              });
            } else {
              callback(null,"nao precisou inserir sinal");
            }
          }
         ,function(callback) { // Atualiza ultimo sinal na tabela de usuario ou horario do ultimo sinal
            if (!!sinal.id_sinal) {
              conn.query("update ge_usuario "
                       + "set id_ultimo_sinal = ? "
                       + "where id_usuario = ?; "
                       + "update ge_equipamento "
                       + "set id_ultimo_sinal = ? "
                       + "where id_equipamento = ?"
                        ,[sinal.id_sinal, v_id_usuario, sinal.id_sinal, v_id_equipamento], function(err, rows, fields) {
                if (!!err) callback("Erro ao inserir sinal ! ERR: " + err);
                callback(null,"atualizou ultimo ponto");
              });
            } else if (!!sinal.ultimoSinal) {
              conn.query("update ge_sinal  "
                       + "set data_servidor = now()"
                       + "   ,data_sinal = str_to_date(?,'%d/%m/%Y %H:%i:%S')"
                       + "where id_sinal = ?"
                        ,[sinal.data, sinal.ultimoSinal.id_sinal], function(err, rows, fields) {
                if (!!err) callback("Erro ao inserir sinal ! ERR: " + err);
                callback(null,"atualizou horario do ponto");
              });
            } else {
              callback(null,"algo estranho aconteceu");
            }
         }
        ], function(err, results) { // Tratamento de erros
             conn.end();
             console.log(atual.codigo + ' - Fila de ações: ' + results);
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

function verificaEndereco() {
  var conn, sinal = [];
  async.series([
    function(callback) { // conectar BD
      conn = getConexaoDB();
      conn.connect(function(err) {
        if (!!err) callback("Erro ao conectar com o banco de dados ! ERR: " + err);
        //conn.on('error',function(err){console.log(err.code)});
        callback(null,'conectou');
      });
    }
   ,function(callback) { // pegar lista de sinais sem endereco
      conn.query("select id_sinal, latitude, longitude "
               + "from ge_sinal "
               + "where pais is null "
               + "limit 0, 500"
                ,function(err, rows, fields) {
        if (!!err) callback("Erro ao listar sinais sem endereco ! ERR: " + err);
          if (rows.length == 0)
            callback(null,'nao possui ultimo sinal');
          else {
            for (var i = 0; i < rows.length; i++)
              sinal.push({id_sinal: rows[i].id_sinal
                         ,lat: rows[i].latitude
                         ,lng: rows[i].longitude});
            callback(null,"listou sinais sem endereço");
          }
      });
    }
   ,function(callback) {
      var count = 0, enderecos = [], stm = "";
        if (sinal.length > 0) {
          for (var i = 0; i < sinal.length; i++) {
            setTimeout(function(sinalAtual) {
              reverseGeocode(sinalAtual.lat, sinalAtual.lng, function(end, id_sinal) {
                enderecos.push({end: end, id_sinal: id_sinal});
                if (++count == sinal.length) {
                  for (var n = 0; n < enderecos.length; n++) {
                    stm += "update ge_sinal "
                         + "set logradouro = " + conn.escape(enderecos[n].end.logradouro)
                               + ", numero = " + conn.escape(enderecos[n].end.numero)
                               + ", bairro = " + conn.escape(enderecos[n].end.bairro)
                               + ", cidade = " + conn.escape(enderecos[n].end.cidade)
                               + ", estado = " + conn.escape(enderecos[n].end.estado)
                                 + ", pais = " + conn.escape(enderecos[n].end.pais)
                                  + ", cep = " + conn.escape(enderecos[n].end.cep)
                        + " where id_sinal = " + conn.escape(enderecos[n].id_sinal)
                        + "; ";
                  }
                  conn.query(stm, function(err, rows, fields) {
                    if (!!err) callback("Erro ao listar sinais sem endereco ! ERR: " + err);
                    console.log(count + " sinais atualizados");
                    callback(null,"atualizou sinais");
                  });
                }
              }, sinalAtual.id_sinal);
            }, 400 * i, sinal[i]);
          }
        } else {
          callback(null,"nao precisou atualizar nenhum registro");
        }
    }
  ], function(err, results) { // Tratamento de erros
       conn.end();
       setTimeout(verificaEndereco, 120000);
  });
}

function getConexaoDB() {
  return mysql.createConnection(DBConfigs);
}

// Validação dos dados recebidos
function checarDados(sinal) {
  if (!sinal.imei) return "IMEI não localizado!";
  if (!sinal.user) return "Usuário não localizado!";
  if (!sinal.data) return "Longitude não localizada!";
  if (!sinal.coord) return "Coordenadas não localizadas!";
  if (!sinal.coord.lat) return "Latitude não localizada!";
  if (!sinal.coord.lng) return "Longitude não localizada!";
  return null;
}

function geraNovoCodigo() {
  return Math.ceil(Math.random() * 9999999).toString();
}

function distancia(lat1, lng1, lat2, lng2) { // retorna distancia entre dois pontos em metros
  var raio_terra = 6378136.245 // em Metros
     ,rad        = 180 / Math.PI
     ,arco_ab    = 90 - lat1
     ,arco_ac    = 90 - lat2
     ,arco_abc   = lng2 - lng1
     ,arco_cos;

  arco_cos = (Math.cos(arco_ac/rad) * Math.cos(arco_ab/rad)) + (Math.sin(arco_ac/rad) * Math.sin(arco_ab/rad) * Math.cos(arco_abc/rad));
  arco_cos = (Math.acos(arco_cos) * 180) / Math.PI;

  return Math.round((2 * Math.PI * raio_terra * arco_cos) / 360);
}

function reverseGeocode(p_lat, p_lng, p_retorno, p_sinal) {
  var options = {
        host: 'maps.google.com',
        port: 80,
        path: '/maps/api/geocode/json?latlng=' + p_lat + ',' + p_lng + '&sensor=false'
      }
     ,resposta = "";

  http.get(options, function(res) {
    res.on('data', function(chunk){
      resposta += chunk.toString('UTF8');
    });

    res.on('end', function() {
      processar(resposta, p_retorno);
    });

  }).on('error', function(e) {
    console.log("Got error: " + e.message);
  });

  function processar(json, p_retorno) {
    json = JSON.parse(json);
    var endereco = {endereco:"", pais:"", estado:"", cidade:"", bairro:"", logradouro:"", numero:"", cep:""}, results;
    if (json.status === 'OK') {
      endereco.endereco = json.results[0].formatted_address;
      results = json.results[0].address_components;
      for (var i  = 0; i < results.length; i++){
        var types = "," + results[i].types.join() + ",";
        if (!!~types.indexOf(",country,")) {
          endereco.pais = results[i].long_name;
        } else if (!!~types.indexOf(",administrative_area_level_1,")) {
          endereco.estado = results[i].long_name;
        } else if (!!~types.indexOf(",locality,")) {
          endereco.cidade = results[i].long_name;
        } else if (!!~types.indexOf(",sublocality,")) {
          endereco.bairro = results[i].long_name;
        } else if (!!~types.indexOf(",route,")) {
          endereco.logradouro = results[i].long_name;
        } else if (!!~types.indexOf(",street_number,")) {
          endereco.numero = results[i].long_name;
        } else if (!!~types.indexOf(",postal_code,")) {
          endereco.cep = results[i].long_name;
        }
      }
    } else if (json.status === 'ZERO_RESULTS') {
    } else console.log('erro ao geocodificar: ' + json.status);
    if (typeof p_retorno === "function")
      p_retorno(endereco, p_sinal);
  }
}