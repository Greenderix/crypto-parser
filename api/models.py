from pydantic import BaseModel


class CourseModel(BaseModel):
    direction: str
    value: float


class CoursesModel(BaseModel):
    exchanger: str
    courses: list[CourseModel]
