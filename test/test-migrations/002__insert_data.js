export default async ([client]) => {
  await client.execute(
    "INSERT INTO users (id, name) VALUES (uuid(), 'Alice')"
  );
};
