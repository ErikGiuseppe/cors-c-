using Dapper;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Mvc;
using Microsoft.VisualBasic;
using MySqlConnector;
using Newtonsoft.Json;
using StackExchange.Redis;
using System.Text;
using web_app_domain;
using web_app_repository;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System.ComponentModel.DataAnnotations.Schema;

namespace web_app_performance.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class ProdutoController : ControllerBase
    {
        private static ConnectionMultiplexer redis;
        private readonly IProdutoRepository _repository;

        public ProdutoController(IProdutoRepository repository)
        {
            _repository = repository;
        }
        [HttpGet]
        public async Task<IActionResult> GetProduto()
        {
            string key = "getproduto";
            redis = ConnectionMultiplexer.Connect("localhost:6379");
            IDatabase db = redis.GetDatabase();
            await db.KeyExpireAsync(key, TimeSpan.FromSeconds(10));
            string user =await db.StringGetAsync(key);
            if (!string.IsNullOrEmpty(user)) {
                return Ok(user);
            
            }
            var produtos = await _repository.ListarProdutos();
            string produtosJson = JsonConvert.SerializeObject(produtos);
            await db.StringSetAsync(key,produtosJson);

            return Ok(produtos);
        }
        [HttpPost]
        public async Task<IActionResult> Post([FromBody] Produto produto)
        {
            await _repository.SalvarProduto(produto);

            string key = "getproduto";
            redis = ConnectionMultiplexer.Connect("localhost:6379");
            IDatabase db = redis.GetDatabase();
            await db.KeyDeleteAsync(key);

            return Ok();
        }

        [HttpPut]
        public async Task<IActionResult> Put([FromBody] Produto produto)
        {
            await _repository.AtualizarProduto(produto);

            string key = "getproduto";
            redis = ConnectionMultiplexer.Connect("localhost:6379");
            IDatabase db = redis.GetDatabase();
            await db.KeyDeleteAsync(key);

            return Ok();
        }
        [HttpDelete]
        public async Task<IActionResult> Delete(int id)
        {
            await _repository.RemoverProduto(id);

            string key = "getproduto";
            redis = ConnectionMultiplexer.Connect("localhost:6379");
            IDatabase db = redis.GetDatabase();
            await db.KeyDeleteAsync(key);

            return Ok();
        }
        [HttpPost("/produtor")]
        public async Task<IActionResult> Produtor([FromBody] Produto produto) 
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost"
            };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            const string fila = "produto_cadastrado";
            channel.QueueDeclare(queue: fila, durable: false, exclusive: false, autoDelete: false, arguments: null);

           
            var mensagemJson = JsonConvert.SerializeObject(produto);
            var body = Encoding.UTF8.GetBytes(mensagemJson);
            channel.BasicPublish(exchange: "", routingKey: fila, basicProperties: null, body: body);
            return Ok();

        }
        [HttpGet("/consumer")]
        public async Task<IActionResult> Consumer()
        {
            var factory = new ConnectionFactory()
            {
                HostName = "localhost"
            };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();
            const string fila = "produto_cadastrado";
            channel.QueueDeclare(queue: fila, durable: false, exclusive: false, autoDelete: false, arguments: null);
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, args) =>
            {
                var body = args.Body.ToArray();
                var mensagem = Encoding.UTF8.GetString(body);

                var produto = JsonConvert.DeserializeObject<Produto>(mensagem);

                if (produto != null)
                {
              
                    Console.WriteLine($"Produto cadastrado: {produto.Nome}, Quantidade Inicial: {produto.QuantidadeEstoque}");
                    
                }
                else
                {
                    Console.WriteLine("Produto inválido recebido.");
                }
            };
            channel.BasicConsume(queue: fila, autoAck: true, consumer: consumer);
            return Ok();

        }
    }
}
