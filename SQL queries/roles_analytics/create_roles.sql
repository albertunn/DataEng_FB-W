-- Create roles
CREATE ROLE IF NOT EXISTS analyst_full;
CREATE ROLE IF NOT EXISTS analyst_limited;

-- Example users (optional)
CREATE USER IF NOT EXISTS user_full IDENTIFIED WITH plaintext_password BY 'full_pw';
CREATE USER IF NOT EXISTS user_limited IDENTIFIED WITH plaintext_password BY 'limited_pw';

-- Assign roles to the users
GRANT analyst_full TO user_full;
GRANT analyst_limited TO user_limited;

-- FULL role can query full analytical view
GRANT SELECT ON default_analytics.v_matches_full TO analyst_full;
GRANT SELECT ON default_marts.fact_match TO analyst_full;

-- LIMITED role can only query pseudonymized view
GRANT SELECT ON default_analytics.v_matches_limited TO analyst_limited;
GRANT SELECT ON default_marts.fact_match TO analyst_limited;
