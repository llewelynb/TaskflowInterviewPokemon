# candidate_solution.py
import sqlite3
import os
from fastapi import FastAPI, HTTPException
from typing import List, Optional
import uvicorn
import requests
import difflib

# --- Constants ---
DB_NAME = "pokemon_assessment.db"

# --- Database Connection ---
def connect_db() -> Optional[sqlite3.Connection]:
    """
    Task 1: Connect to the SQLite database.
    Implement the connection logic and return the connection object.
    Return None if connection fails.
    """
    if not os.path.exists(DB_NAME):
        print(f"Error: Database file '{DB_NAME}' not found.")
        return None

    connection = None
    try:
        # --- Implement Here ---
        # I connect to the DB we created and then return the object if successful
        conn = sqlite3.connect(DB_NAME)
        return conn
        # --- End Implementation ---
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return None

    return connection

# --- Data Cleaning ---
def clean_database(conn: sqlite3.Connection):
    """
    Task 2: Clean up the database using the provided connection object.
    Implement logic to:
    - Remove duplicate entries in tables (pokemon, types, abilities, trainers).
      Choose a consistent strategy (e.g., keep the first encountered/lowest ID).
    - Correct known misspellings (e.g., 'Pikuchu' -> 'Pikachu', 'gras' -> 'Grass', etc.).
    - Standardize casing (e.g., 'fire' -> 'Fire' or all lowercase for names/types/abilities).
    """
    if not conn:
        print("Error: Invalid database connection provided for cleaning.")
        return

    cursor = conn.cursor()
    print("Starting database cleaning...")

    try:
        # --- Implement Here ---
        # Correct misspellings using the pokemon API to fetch the list of official names.
        # I did this first, so the duplicates can be removed.
        try:
            apiapiresponse = requests.get("https://pokeapi.co/api/v2/pokemon?limit=2000")
            apiapiresponse.raise_for_status()
            official_list = [ entry["name"] for entry in apiapiresponse.json().get("results", []) ]
        except Exception as ex:
            print(f"Official list not fetched: {ex}")
            official_list = []

        cursor.execute("SELECT id, name FROM pokemon")
        db_rows = cursor.fetchall()

        import difflib
        for pokeid, old_name in db_rows:
            old_name_lower = old_name.strip().lower()

            # Do nothing if its in the official list
            if old_name_lower in official_list:
                continue

            # Find a close match if it's not in the list
            matches = difflib.get_close_matches(old_name_lower, official_list, n=1, cutoff=0.8)
            if matches:
                corrected = matches[0]      
                if corrected != old_name_lower:
                    cursor.execute(
                        "UPDATE pokemon SET name = ? WHERE id = ?",
                        (corrected.title(), pokeid)
                    )

        # Remove duplicates from all tables
        cursor.execute(
            """
            DELETE FROM pokemon
            WHERE id NOT IN(
                SELECT MIN(id) FROM pokemon
                GROUP BY LOWER(name)
            );
            """
        )

        cursor.execute(
            """
            DELETE FROM types
            WHERE id NOT IN (
                SELECT MIN(id) FROM types
                GROUP BY LOWER(name)
            );
            """
        )

        cursor.execute(
            """
            DELETE FROM abilities
            WHERE id NOT IN (
                SELECT MIN(id) FROM abilities
                GROUP BY LOWER(name)
            );
            """
        )

        cursor.execute(
            """
            DELETE FROM trainers
            WHERE id NOT IN (
                SELECT MIN(id) FROM trainers
                GROUP BY LOWER(name)
            );
            """
        )

        # Standardise casing. I strip whitesapce and use title() for sentence case
        cursor.execute("SELECT id, name FROM pokemon")
        db_rows = cursor.fetchall()
        for pokeid, pokename in db_rows:
            clean_name = pokename.strip().title()
            cursor.execute(
                "UPDATE pokemon SET name = ? WHERE id = ?",
                (clean_name, pokeid)
            )

        cursor.execute("SELECT id, name FROM types")
        db_rows = cursor.fetchall()
        for typeid, typename in db_rows:
            clean_name = typename.strip().title()
            cursor.execute(
                "UPDATE types SET name = ? WHERE id = ?",
                (clean_name, typeid)
            )

        cursor.execute("SELECT id, name FROM abilities")
        db_rows = cursor.fetchall()
        for abilityid, abilityname in db_rows:
            clean_name = abilityname.strip().title()
            cursor.execute(
                "UPDATE abilities SET name = ? WHERE id = ?",
                (clean_name, abilityid)
            )

        cursor.execute("SELECT id, name FROM trainers")
        db_rows = cursor.fetchall()
        for trainerid, trainername in db_rows:
            clean_name = trainername.strip().title()
            cursor.execute(
                "UPDATE trainers SET name = ? WHERE id = ?",
                (clean_name, trainerid)
            )

        # Remove redundant data. It looks like this is only applicable in types,
        # but I wrote the code to check everywhere since it is not specified in the instructions.
        cursor.execute(
            """
            DELETE FROM pokemon
            WHERE name IN ('---', '???', '')
            """
        )
        cursor.execute(
            """
            DELETE FROM types
            WHERE name IN ('---', '???', '')
            """
        )
        cursor.execute(
            """
            DELETE FROM abilities
            WHERE name IN ('---', '???', '')
            """
        )
        cursor.execute(
            """
            DELETE FROM trainers
            WHERE name IN ('---', '???', '')
            """
        )

        # --- End Implementation ---
        conn.commit()
        print("Database cleaning finished and changes committed.")

    except sqlite3.Error as e:
        print(f"An error occurred during database cleaning: {e}")
        conn.rollback()  # Roll back changes on error

# --- FastAPI Application ---
def create_fastapi_app() -> FastAPI:
    """
    FastAPI application instance.
    Define the FastAPI app and include all the required endpoints below.
    """
    print("Creating FastAPI app and defining endpoints...")
    app = FastAPI(title="Pokemon Assessment API")

    # --- Define Endpoints Here ---
    @app.get("/")
    def read_root():
        """
        Task 3: Basic root apiresponse message
        Return a simple JSON apiresponse object that contains a `message` key with any corapiresponseonding value.
        """
        # --- Implement here ---
        return {"message": "Custom Pokemon API is working."}

        # --- End Implementation ---

    @app.get("/pokemon/ability/{ability_name}", response_model=List[str])
    def get_pokemon_by_ability(ability_name: str):
        """
        Task 4: Retrieve all Pokemon names with a specific ability.
        Query the cleaned database. Handle cases where the ability doesn't exist.
        """
        # --- Implement here ---
        conn = connect_db()
        if not conn:
            raise HTTPException(status_code=500, detail="DB connection failed.")

        cursor = conn.cursor()
        query = """
            SELECT DISTINCT p.name
            FROM pokemon p
            JOIN trainer_pokemon_abilities tpa ON p.id = tpa.pokemon_id
            JOIN abilities a ON a.id = tpa.ability_id
            WHERE LOWER(a.name) = LOWER(?)
        """
        cursor.execute(query, (ability_name,))
        db_rows = cursor.fetchall()
        conn.close()

        if not db_rows:
            return []

        return [db_row[0] for db_row in db_rows]
        # --- End Implementation ---

    @app.get("/pokemon/type/{type_name}", response_model=List[str])
    def get_pokemon_by_type(type_name: str):
        """
        Task 5: Retrieve all Pokemon names of a specific type (considers type1 and type2).
        Query the cleaned database. Handle cases where the type doesn't exist.
        """
        # --- Implement here ---
        conn = connect_db()
        if not conn:
            raise HTTPException(status_code=500, detail="DB connection failed.")

        cursor = conn.cursor()
        query = """
            SELECT DISTINCT p.name
            FROM pokemon p
            LEFT JOIN types t1 ON p.type1_id = t1.id
            LEFT JOIN types t2 ON p.type2_id = t2.id
            WHERE LOWER(t1.name) = LOWER(?)
               OR LOWER(t2.name) = LOWER(?)
        """
        cursor.execute(query, (type_name, type_name))
        db_rows = cursor.fetchall()
        conn.close()

        return [db_row[0] for db_row in db_rows] if db_rows else []
        # --- End Implementation ---

    @app.get("/trainers/pokemon/{pokemon_name}", response_model=List[str])
    def get_trainers_by_pokemon(pokemon_name: str):
        """
        Task 6: Retrieve all trainer names who have a specific Pokemon.
        Query the cleaned database. Handle cases where the Pokemon doesn't exist or has no trainer.
        """
        # --- Implement here ---
        conn = connect_db()
        if not conn:
            raise HTTPException(status_code=500, detail="DB connection failed.")

        cursor = conn.cursor()
        query = """
            SELECT DISTINCT tr.name
            FROM trainers tr
            JOIN trainer_pokemon_abilities tpa ON tr.id = tpa.trainer_id
            JOIN pokemon p ON p.id = tpa.pokemon_id
            WHERE LOWER(p.name) = LOWER(?)
        """
        cursor.execute(query, (pokemon_name,))
        db_rows = cursor.fetchall()
        conn.close()

        return [db_row[0] for db_row in db_rows] if db_rows else []
        # --- End Implementation ---

    @app.get("/abilities/pokemon/{pokemon_name}", response_model=List[str])
    def get_abilities_by_pokemon(pokemon_name: str):
        """
        Task 7: Retrieve all ability names of a specific Pokemon.
        Query the cleaned database. Handle cases where the Pokemon doesn't exist.
        """
        # --- Implement here ---
        conn = connect_db()
        if not conn:
            raise HTTPException(status_code=500, detail="DB connection failed.")

        cursor = conn.cursor()
        query = """
            SELECT DISTINCT a.name
            FROM abilities a
            JOIN trainer_pokemon_abilities tpa ON a.id = tpa.ability_id
            JOIN pokemon p ON p.id = tpa.pokemon_id
            WHERE LOWER(p.name) = LOWER(?)
        """
        cursor.execute(query, (pokemon_name,))
        db_rows = cursor.fetchall()
        conn.close()

        return [db_row[0] for db_row in db_rows] if db_rows else []
        # --- End Implementation ---

    # --- Implement Task 8 here ---
    @app.post("/pokemon/create/{pokemon_name}")
    def create_pokemon(pokemon_name: str):
        import requests

        conn = connect_db()
        if not conn:
            raise HTTPException(status_code=500, detail="DB connection failed.")

        cursor = conn.cursor()

        # Use the public pokemon API to find the pokemon's info
        poke_url = f"https://pokeapi.co/api/v2/pokemon/{pokemon_name.lower()}"
        apiresponse = requests.get(poke_url)
        if apiresponse.status_code != 200:
            raise HTTPException(status_code=404, detail="Pokemon not found.")
        data = apiresponse.json()

        # Get the pokemon's types
        type_ids = []
        for type_entry in data.get("types", []):
            type_name = type_entry["type"]["name"].strip().lower()

            cursor.execute("SELECT id FROM types WHERE LOWER(name) = LOWER(?)", (type_name,))
            trow = cursor.fetchone()

            if trow:
                type_id = trow[0]
            else:
                cursor.execute("INSERT INTO types (name) VALUES (?)", (type_name.title(),))
                type_id = cursor.lastrowid
            type_ids.append(type_id)

        type1_id = type_ids[0] if len(type_ids) >= 1 else None
        type2_id = type_ids[1] if len(type_ids) >= 2 else None

        # If the pokemon exists, get the id, otherwise create it
        cursor.execute(
            "SELECT id FROM pokemon WHERE LOWER(name) = LOWER(?)",
            (pokemon_name,)
        )
        db_row = cursor.fetchone()
        if db_row:
            pokemon_id = db_row[0]
            # If the type has changed in the mean time, update it
            cursor.execute(
                "UPDATE pokemon SET type1_id = ?, type2_id = ? WHERE id = ?",
                (type1_id, type2_id, pokemon_id)
            )
        else:
            cursor.execute(
                """
                INSERT INTO pokemon (name, type1_id, type2_id)
                VALUES (?, ?, ?)
                """,
                (pokemon_name.title(), type1_id, type2_id)
            )
            pokemon_id = cursor.lastrowid

        # Do the same with abilities, if it exists, get the id, otherwise create it
        new_tpa_ids = []
        for entry in data.get("abilities", []):
            abi = entry["ability"]
            abi_name = abi["name"].strip().lower()
            cursor.execute(
                "SELECT id FROM abilities WHERE LOWER(name) = LOWER(?)",
                (abi_name,)
            )
            abilityitem = cursor.fetchone()
            if abilityitem:
                ability_id = abilityitem[0]
            else:
                cursor.execute(
                    "INSERT INTO abilities (name) VALUES (?)",
                    (abi_name.title(),)
                )
                ability_id = cursor.lastrowid

            # Assign a random trainer
            cursor.execute("SELECT id FROM trainers ORDER BY RANDOM() LIMIT 1")
            trow = cursor.fetchone()
            if not trow:
                conn.rollback()
                conn.close()
                raise HTTPException(status_code=500, detail="No trainers in the table")
            trainer_id = trow[0]

            # create the new record
            cursor.execute(
                """
                INSERT INTO trainer_pokemon_abilities (trainer_id, pokemon_id, ability_id)
                VALUES (?, ?, ?)
                """,
                (trainer_id, pokemon_id, ability_id)
            )
            new_tpa_ids.append(cursor.lastrowid)

        conn.commit()
        conn.close()

        return {"created_tpa_ids": new_tpa_ids}

    # --- End Implementation ---

    print("FastAPI app created successfully.")
    return app


# --- Main execution / Uvicorn setup (Optional - for candidate to run locally) ---
if __name__ == "__main__":
    # Ensure data is cleaned before running the app for testing
    temp_conn = connect_db()
    if temp_conn:
        clean_database(temp_conn)
        temp_conn.close()

    app_instance = create_fastapi_app()
    uvicorn.run(app_instance, host="127.0.0.1", port=8000)
