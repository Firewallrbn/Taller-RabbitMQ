import pika
import json
import random
import time

def generate_order():
    """Genera un pedido aleatorio con datos realistas."""
    customers = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
    products = [
        {"name": "Laptop", "price": 999.99},
        {"name": "Mouse", "price": 29.99},
        {"name": "Keyboard", "price": 59.99},
        {"name": "Monitor", "price": 299.99},
        {"name": "Headphones", "price": 79.99}
    ]
    
    num_items = random.randint(1, 3)
    selected_items = random.sample(products, num_items)
    items_with_qty = [{"product": item["name"], "price": item["price"], "quantity": random.randint(1, 2)} for item in selected_items]
    
    order = {
        "order_id": f"ORD-{random.randint(1000, 9999)}",
        "customer": random.choice(customers),
        "items": items_with_qty,
        "total": round(sum(item["price"] * item["quantity"] for item in items_with_qty), 2),
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S")
    }
    return order

def main():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='orders', durable=True)

    num_orders = 5
    print(f" [*] Sending {num_orders} orders...")
    
    for i in range(num_orders):
        order = generate_order()
        message = json.dumps(order)
        
        channel.basic_publish(
            exchange='',
            routing_key='orders',
            body=message,
            properties=pika.BasicProperties(
                delivery_mode=2,  # Hace el mensaje persistente
            )
        )
        print(f" [{i+1}] Sent order {order['order_id']} for {order['customer']} - Total: ${order['total']}")
        time.sleep(0.5)
    
    print(f" [x] All {num_orders} orders sent successfully!")
    connection.close()

if __name__ == '__main__':
    main()
