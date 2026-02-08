#!/bin/bash
set -e

BASE_URL="http://localhost:8080/api"
COOKIES="cookies.txt"
rm -f $COOKIES

echo "1. Registering User..."
curl -s -c $COOKIES -X POST $BASE_URL/auth/register \
  -H "Content-Type: application/json" \
  -d '{"email":"verifier@sankofa.dev", "password":"password123", "full_name":"Verifier", "organization_name":"Verifier Org"}' > register_response.json
cat register_response.json
if grep -q "token" register_response.json; then
  echo "✅ Registered & Token Received"
else
  echo "❌ Registration Failed"
  exit 1
fi

echo "2. Logging In..."
curl -s -X POST $BASE_URL/auth/login \
  -H "Content-Type: application/json" \
  -d '{"email":"verifier@sankofa.dev", "password":"password123"}' > login_response.json
cat login_response.json
if grep -q "token" login_response.json; then
  echo "✅ Logged In & Token Received"
  # Extract token using grep/sed/awk since we don't have jq guaranteed (or minimal dep)
  # Assuming simpler JSON structure, let's try a basic extraction
  TOKEN=$(cat login_response.json | grep -o '"token":"[^"]*' | cut -d'"' -f4)
  echo "Token: $TOKEN"
else
  echo "❌ Login Failed"
  exit 1
fi

echo "3. Getting Profile..."
curl -s -H "Authorization: Bearer $TOKEN" $BASE_URL/auth/me | grep "email" && echo "✅ Profile Fetched"

echo "4. Getting Organizations..."
ORG_RES=$(curl -s -H "Authorization: Bearer $TOKEN" $BASE_URL/organizations)
echo $ORG_RES
# Extract Org ID
ORG_ID=$(echo $ORG_RES | grep -o '"id":[0-9]*' | head -1 | cut -d: -f2)
echo "Found Org ID: $ORG_ID"

echo "5. Getting Projects..."
curl -s -H "Authorization: Bearer $TOKEN" "$BASE_URL/projects?org_id=$ORG_ID" | grep "name" && echo "✅ Projects Fetched"

echo "6. Creating New Project..."
curl -s -H "Authorization: Bearer $TOKEN" -X POST $BASE_URL/projects \
  -H "Content-Type: application/json" \
  -d "{\"name\":\"Verified Project\", \"org_id\":$ORG_ID}" | grep "Verified Project" && echo "✅ Project Created"
