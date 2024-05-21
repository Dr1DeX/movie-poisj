from dataclasses import dataclass
from enum import Enum
from http import HTTPStatus
from typing import List, Optional
from urllib.parse import urljoin

import requests
from flask import Flask, jsonify, request
from wtforms import Form, IntegerField, SelectField, StringField, validators

app = Flask("movies_service")
BASE_ES_URL = "https://localhost:9200"


def dependency_mock_auth():
    return "elastic", "lolahaha12"


@dataclass
class Actor:
    id: int
    name: str

    def to_dict(self) -> dict:
        return {
            "id": int(self.id),
            "name": self.name,
        }


@dataclass
class Writers:
    id: str
    name: str

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "name": self.name,
        }


@dataclass
class ShortMovie:
    id: str
    title: str
    imdb_rating: float

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "title": self.title,
            "imdb_rating": self.imdb_rating,
        }


@dataclass
class Movie(ShortMovie):
    description: str
    genre: List[str]
    actors: List[Actor]
    writers: List[Writers]
    directors: List[str]

    def to_dict(self) -> dict:
        return {
            **super().to_dict(),
            "description": self.description,
            "genre": self.genre,
            "actors": [a.to_dict() for a in self.actors],
            "writers": [w.to_dict() for w in self.writers],
            "directors": self.directors,
        }


class SortOrder(Enum):
    ASC = "asc"
    DESC = "desc"


class SortField(Enum):
    ID = "id"
    TITLE = "title"
    IMDB_RATING = "imdb_rating"


def get_movie_by_id(movie_id: str) -> Optional[Movie]:
    request_data = {
        "query": {
            "term": {
                "id": {
                    "value": movie_id
                }
            }
        }
    }

    response = requests.get(
        url=urljoin(BASE_ES_URL, 'movies/_search'),
        json=request_data,
        headers={"Content-Type": "application/json"}
    )

    if not response.ok:
        response.raise_for_status()

    data = response.json()
    result = data["hits"]["hits"]

    if not result:
        return None

    movie_row = result[0]['_source']
    movie = Movie(
        id=movie_row["id"],
        title=movie_row["title"],
        imdb_rating=movie_row["imdb_rating"],
        description=movie_row["description"],
        genre=movie_row["genre"],
        actors=[Actor(**x) for x in movie_row["actors"]],
        writers=[Writers(**x) for x in movie_row["writers"]],
        directors=movie_row["directors"]
    )
    return movie


def search_movies(
        *,
        search_query: Optional[str] = None,
        sort_order: SortOrder = SortOrder.ASC,
        sort: SortField = SortField.ID,
        page: int = 1,
        limit: int = 50,
) -> List[ShortMovie]:
    sort_value = sort.value
    if sort_value == SortField.TITLE.value:
        sort_value = f"{SortField.TITLE.value}.raw"

    request_data = {
        "size": limit,
        "from": (page - 1) * limit,
        "sort": [
            {
                sort_value: sort_order.value
            }
        ],
        "_source": ["id", "title", "imdb_rating"],
    }

    if search_query:
        request_data["query"] = {
            "multi_match": {
                "query": search_query,
                "fuzziness": "auto",
                "fields": [
                    "title^5",
                    "description^4",
                    "genre^3",
                    "actors_names^3",
                    "writers_names^2",
                    "director"
                ]
            }
        }

    response = requests.get(
        url=urljoin(BASE_ES_URL, "movies/_search"),
        json=request_data,
        headers={"Content-Type": "application/json"},
        timeout=50,
        auth=dependency_mock_auth(),
        verify=False
    )

    if not response.ok:
        response.raise_for_status()

    data = response.json()
    result = data["hits"]["hits"]
    movies = []
    if result:
        for record in result:
            movie_row = record["_source"]
            movies.append(ShortMovie(
                id=movie_row["id"],
                title=movie_row["title"],
                imdb_rating=movie_row["imdb_rating"]
            ))
    return movies


class SearchMoviesValidator(Form):
    limit = IntegerField("Limit", [validators.NumberRange(min=0)], default=50)
    page = IntegerField("Page", [validators.NumberRange(min=1)], default=1)
    search = StringField("Search", default="")
    sort = SelectField(
        "Sort",
        choices=[
            (SortField.ID.value, SortField.ID.value),
            (SortField.TITLE.value, SortField.TITLE.value),
            (SortField.IMDB_RATING.value, SortField.IMDB_RATING.value)
        ],
        default=SortField.ID.value,
    )
    sort_order = SelectField(
        "SelectField",
        choices=[
            (SortOrder.ASC.value, SortOrder.ASC.value),
            (SortOrder.DESC.value, SortOrder.DESC.value),
        ],
        default=SortOrder.ASC.value
    )


def validation_errors_to_dict(errors: dict) -> List[dict]:
    validation_error = []
    for field_name, field_errors in errors.items():
        for err in field_errors:
            validation_error.append(
                {
                    "loc": [
                        "query",
                        field_name,
                    ],
                    "msg": err
                }
            )
    return validation_error


@app.route("/api/movies", methods=["GET"], strict_slashes=False)
def movies_list():
    form = SearchMoviesValidator(request.args)
    validation_errors = []
    if not form.validate():
        validation_errors = validation_errors_to_dict(form.errors)

    if validation_errors:
        return jsonify(detail=validation_errors), HTTPStatus.UNPROCESSABLE_ENTITY

    movies = search_movies(
        search_query=form.search.data,
        sort_order=SortOrder(form.sort_order.data),
        sort=SortField(form.sort.data),
        page=form.page.data,
        limit=form.limit.data
    )
    return jsonify([m.to_dict() for m in movies])


if __name__ == '__main__':
    app.run(port=8000)
