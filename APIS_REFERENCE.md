## 📄 **APIS_REFERENCE.md**

```markdown
# Public APIs Reference

This document lists the public APIs used in the **Kafka n APIs** project. Each entry includes a description and a Python `requests` example.

---

## 1. JSONPlaceholder
- **URL:** https://jsonplaceholder.typicode.com/
- **Description:** Fake JSON API for testing
- **GET Example:**
  ```python
  response = requests.get("https://jsonplaceholder.typicode.com/posts")
  return response.json()
  ```

## 2. Fake Store API
- **URL:** https://fakestoreapi.com/
- **Description:** Fake e-commerce API
- **GET Example:**
  ```python
  response = requests.get('https://fakestoreapi.com/products')
  return response.json()
  ```

## 3. DummyJSON
- **URL:** https://dummyjson.com/
- **Description:** Fake data for testing
- **GET Example:**
  ```python
  response = requests.get('https://dummyjson.com/products')
  return response.json()
  ```

## 4. httpbin.org
- **URL:** https://httpbin.org/
- **Description:** HTTP request testing service
- **GET Example:**
  ```python
  response = requests.get("https://httpbin.org/get")
  return response.json()
  ```

## 5. No-as-a-Service
- **URL:** https://naas.isalman.dev
- **Description:** API that always returns a negative response, simulating an unreliable service
- **GET Example:**
  ```python
  response = requests.get("https://naas.isalman.dev/no")
  return response.json()
  ```

## 6. Frankfurter
- **URL:** https://www.frankfurter.app/
- **Description:** Free and open-source currency exchange rate API 💸
- **GET Example:**
  ```python
  response = requests.get("https://api.frankfurter.dev/v2/rates")
  return response.json()
  ```

## 7. SpaceX REST API
- **URL:** https://github.com/r-spacex/SpaceX-API
- **Description:** Historical and real-time data about SpaceX launches 🚀
- **GET Example:**
  ```python
  response = requests.get("https://api.spacexdata.com/v5/launches/latest")
  return response.json()
  ```

## 8. Open Library
- **URL:** https://openlibrary.org/developers/api
- **Description:** Open digital library with access to books, authors, and metadata 📚
- **GET Example:**
  ```python
  response = requests.get("http://openlibrary.org/search/lists.json?q=book&limit=20&offset=0")
  return response.json()
  ```

## 9. Hacker News API
- **URL:** https://github.com/HackerNews/API
- **Description:** Programmatic feed of news and discussions from Y Combinator Hacker News 📰
- **GET Example:**
  ```python
  response = requests.get("https://hacker-news.firebaseio.com/v0/newstories.json?print=pretty&limit=200")
  return response.json()
  ```

## 10. 4Devs
- **URL:** https://www.4devs.com.br/
- **Description:** Online tools (CPF generator)
- **POST Example:**
  ```python
  response = requests.post(
      "https://www.4devs.com.br/ferramentas_online.php",
      data="acao=gerar_cpf&pontuacao=N&cpf_estado=",
      headers={"Content-Type": "application/x-www-form-urlencoded"}
  )
  print(response.text)
  ```

## 11. IP Query IO
- **URL:** https://ipquery.io/
- **Description:** API providing information about an IP address, including geolocation and VPN detection
- **GET Example:**
  ```python
  response = requests.get("https://api.ipquery.io/")
  return response.json()
  ```

## 12. AwesomeAPI
- **URL:** https://docs.awesomeapi.com.br/api-de-moedas
- **Description:** Real-time currency exchange API with over 150 currencies!
- **GET Example:**
  ```python
  response = requests.get("https://economia.awesomeapi.com.br/json/available")
  return response.json()
  ```

## 13. UUID Generator API
- **URL:** https://www.uuidtools.com/docs?ref=freepublicapis.com
- **Description:** Generates unique UUIDs and GUIDs via HTTP requests
- **GET Example:**
  ```python
  response = requests.get("https://www.uuidtools.com/api/generate/v1")
  return response.json()
  ```

## 14. TheMealDB
- **URL:** https://www.themealdb.com/documentation
- **Description:** Meal recipe API for developers and food lovers
- **GET Example:**
  ```python
  response = requests.get("https://www.themealdb.com/api/json/v1/1/categories.php")
  return response.json()
  ```

## 15. Webhook Site
- **URL:** https://webhook-test.com/
- **Description:** Webhook simulation site
- **GET Example:**
  ```python
  response = requests.get("https://webhook.site/b7de792c-2bca-49af-91d4-e768a103ab61")
  return response.json()
  ```

---

**Note:** All examples use the `requests` library. Install it with `pip install requests` if needed.
```