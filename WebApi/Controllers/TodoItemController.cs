using Microsoft.AspNetCore.Mvc;
using WebApi.Contexts;
using WebApi.Extensions;
using WebApi.Models;

namespace WebApi.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class TodoItemController : ControllerBase
    {
        private readonly AppDbContext _appDbContext;

        public TodoItemController(AppDbContext appDbContext)
        {
            _appDbContext = appDbContext;
        }

        [HttpPost("Salvar")]
        public ActionResult Salvar()
        {
            var items = new List<TodoItem>
            {
                new() {
                    Name = "Teste 1",
                    IsComplete = true,
                },
                new() {
                    Name = "Teste 2",
                    IsComplete = false,
                }
            };

            _appDbContext.BulkInsert(items, 1000);

            return Ok();
        }

        [HttpPut]
        public ActionResult Atualizar()
        {
            var items = new List<TodoItem>
            {
                new() {
                    Id = 1,
                    Name = "Teste 1 - Updated",
                    IsComplete = true,
                },
                new() {
                    Id = 2,
                    Name = "Teste 2 - Updated",
                    IsComplete = true,
                }
            };

            _appDbContext.BulkUpdate(items, 1000);

            return Ok();
        }

    }
}
