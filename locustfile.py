from locust import HttpUser, task, between

class MyUser(HttpUser):
    wait_time = between(0, 0.00000005)

    @task
    def fetch_courses(self):
        self.client.get("/courses")
