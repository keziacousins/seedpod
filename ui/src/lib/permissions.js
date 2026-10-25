/**
 * API key permissions, as the API sends and accepts them.
 *
 * `POST /api/keys` takes `permissions` as a list of strings, and every key
 * response returns one. The key form keeps its checkboxes as an object,
 * `{ "clusters:read": true }`, and used to send that object as it was. The API
 * refused it with a 422, so no key could be made from the form.
 */

/** The checked permissions in the form's checkbox object, as the list to send. */
export function selectedPermissions(checkboxes) {
  return Object.keys(checkboxes).filter((permission) => checkboxes[permission]);
}
