/** @type {import('ts-jest').JestConfigWithTsJest} */
module.exports = {
  preset: 'ts-jest',
  testEnvironment: 'jsdom',
  moduleNameMapper: {
    "^SnubaAdmin/(.*)$": "<rootDir>/static/$1"
  }
};
